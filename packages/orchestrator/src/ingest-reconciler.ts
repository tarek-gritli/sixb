import type { Queues } from "@sixb/core"
import { reportDatasetIngestFlushFailure } from "@sixb/core/internal/error-reporting"
import { captureSixbFailure } from "@sixb/core/internal/errors"
import type { DatasetIngestSummary } from "@sixb/core/storage"
import type { IngestReconciliationPorts } from "./types"

const INTERVAL_MS = 30_000

/** Changes that waited this long with no failed flush mean no flush runs for the dataset. */
const STALLED_AFTER_MS = 10 * 60_000

interface IngestReconcilerInput extends IngestReconciliationPorts {
  readonly projectId: string
  readonly queues: Pick<Queues, "lake">
}

/**
 * Recovers the flush requests the live `dataset.ingest.queued` path can lose: a process that
 * stopped between storing a delivery and announcing it, a broker that dropped the event, or an
 * orchestrator that was not running. Each pass queues a flush for every dataset whose deliveries
 * wait with no flush holding it, under the request the inbox holds, so a request the live path
 * already queued queues nothing more.
 *
 * A dataset whose oldest delivery waited for long with no failed flush is reported once: changes
 * are stored, and nothing writes them, such as in a deployment that runs no sync worker.
 */
export async function runIngestReconciler(
  input: IngestReconcilerInput,
  signal: AbortSignal
): Promise<void> {
  const reported = new Set<string>()
  const intervalMs = input.intervalMs ?? INTERVAL_MS

  while (!signal.aborted) {
    try {
      await reconcile(input, reported)
    } catch (error) {
      console.error(
        `[SixbOrchestrator] Ingest reconciliation could not read the inbox (projectId=${input.projectId}):`,
        error
      )
    }
    await waitForNextPass(intervalMs, signal)
  }
}

async function reconcile(input: IngestReconcilerInput, reported: Set<string>): Promise<void> {
  const summaries = await input.inbox.summarize({ projectId: input.projectId })
  const now = Date.now()

  for (const summary of summaries) {
    if (summary.pendingDeliveries === 0) continue

    if (summary.leaseExpiresAt === null) {
      await enqueueIngestFlush(input.queues, input.projectId, summary, {
        source: "ingest-reconciliation",
      })
    }

    reportStalled(input, summary, now, reported)
  }
}

function reportStalled(
  input: IngestReconcilerInput,
  summary: DatasetIngestSummary,
  now: number,
  reported: Set<string>
): void {
  const oldest = summary.oldestReceivedAt
  if (oldest === null || summary.failureStreak > 0) return
  if (now - oldest.getTime() < STALLED_AFTER_MS) return

  // One report per delivery that waits, not one per pass.
  const subject = `stalled:${oldest.toISOString()}`
  const key = `${summary.datasetId}:${subject}`
  if (reported.has(key)) return
  reported.add(key)

  const minutes = Math.floor((now - oldest.getTime()) / 60_000)
  const error = new Error(
    `[SixbOrchestrator] Dataset '${summary.datasetId}' has ${summary.pendingDeliveries} ingested delivery(ies) waiting for ${minutes} min with no flush. Run a sync worker for this project.`
  )

  reportDatasetIngestFlushFailure(input.reporter, error, {
    projectId: input.projectId,
    datasetId: summary.datasetId,
    failure: captureSixbFailure(error, {
      allowedCodes: ["internal.unexpected", "runtime.cancelled"],
      defaultCode: "internal.unexpected",
    }),
    failureStreak: 0,
    pendingDeliveries: summary.pendingDeliveries,
    oldestReceivedAt: oldest.toISOString(),
    subject,
  })
}

/** Queues the flush a request names: one job per request, whoever asks. */
export async function enqueueIngestFlush(
  queues: Pick<Queues, "lake">,
  projectId: string,
  request: { readonly datasetId: string; readonly request: number },
  metadata: Readonly<Record<string, string>>
): Promise<void> {
  await queues.lake.enqueue({
    projectId,
    jobs: [
      {
        id: `ingest-flush:${request.datasetId}:${request.request}`,
        type: "dataset.ingest.flush.requested",
        payload: { datasetId: request.datasetId },
        metadata,
      },
    ],
  })
}

async function waitForNextPass(intervalMs: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return
  await new Promise<void>((resolve) => {
    const timer = setTimeout(finish, intervalMs)
    function finish(): void {
      clearTimeout(timer)
      signal.removeEventListener("abort", finish)
      resolve()
    }
    signal.addEventListener("abort", finish, { once: true })
  })
}
