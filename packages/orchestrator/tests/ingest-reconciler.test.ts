import { afterEach, expect, jest, test } from "bun:test"
import { InMemoryQueues, type SixbErrorContext } from "@sixb/core"
import { attachSixbErrorReporter, flushSixbErrors } from "@sixb/core/internal/error-reporting"
import type { DatasetIngestSummary } from "@sixb/core/storage"
import { runIngestReconciler } from "../src/ingest-reconciler"

const PROJECT_ID = "ingest-reconciliation"
const MINUTE = 60_000

afterEach(() => {
  jest.useRealTimers()
})

function summary(
  input: Partial<DatasetIngestSummary> & { datasetId: string }
): DatasetIngestSummary {
  return {
    pendingDeliveries: 1,
    pendingChanges: 1,
    oldestReceivedAt: new Date(),
    request: 0,
    leaseExpiresAt: null,
    failureStreak: 0,
    ...input,
  }
}

async function queuedFlushes(queues: InMemoryQueues): Promise<string[]> {
  const claimed = await queues.lake.claim({ projectId: PROJECT_ID, workerId: "test", limit: 10 })
  return claimed.map(({ job }) => job.id)
}

async function turns(count = 20): Promise<void> {
  for (let turn = 0; turn < count; turn += 1) {
    await new Promise<void>((resolve) => setImmediate(resolve))
  }
}

// Reproduce: skip the enqueue in `reconcile` (ingest-reconciler.ts); the waiting dataset gets no job.
test("queues a flush for each dataset whose deliveries wait with no flush holding it", async () => {
  const queues = new InMemoryQueues()
  const controller = new AbortController()
  const inbox = {
    summarize: async () => [
      summary({ datasetId: "crm.people", request: 4 }),
      // A flush holds this one: it writes the deliveries itself.
      summary({ datasetId: "crm.orgs", leaseExpiresAt: new Date(Date.now() + MINUTE) }),
      // A lease whose flush has finished and found nothing pending.
      summary({ datasetId: "crm.deals", pendingDeliveries: 0 }),
    ],
  }

  const running = runIngestReconciler(
    { projectId: PROJECT_ID, queues, inbox, reporter: {} },
    controller.signal
  )
  await turns()
  controller.abort()
  await running

  expect(await queuedFlushes(queues)).toEqual(["ingest-flush:crm.people:4"])
})

// Reproduce: drop the `reported` check in `reportStalled`; each pass reports again.
test("reports a dataset whose changes wait with no flush, once per waiting delivery", async () => {
  jest.useFakeTimers()
  const host = {}
  const reports: SixbErrorContext[] = []
  attachSixbErrorReporter(host, (_error, context) => {
    reports.push(context)
  })

  const stalledSince = new Date(Date.now() - 11 * MINUTE)
  const inbox = {
    summarize: async () => [
      summary({ datasetId: "crm.people", pendingDeliveries: 3, oldestReceivedAt: stalledSince }),
      // Flushes run and fail: the flush reports those itself.
      summary({ datasetId: "crm.orgs", oldestReceivedAt: stalledSince, failureStreak: 2 }),
      // Waiting, but not for long.
      summary({ datasetId: "crm.deals", oldestReceivedAt: new Date(Date.now() - MINUTE) }),
    ],
  }
  const controller = new AbortController()
  const running = runIngestReconciler(
    {
      projectId: PROJECT_ID,
      queues: new InMemoryQueues(),
      inbox,
      reporter: host,
      intervalMs: MINUTE,
    },
    controller.signal
  )

  await turns()
  jest.advanceTimersByTime(MINUTE)
  await turns()
  controller.abort()
  await running
  await flushSixbErrors(host)

  expect(reports).toEqual([
    expect.objectContaining({
      type: "dataset.ingest.flush.failed",
      projectId: PROJECT_ID,
      datasetId: "crm.people",
      failureStreak: 0,
      pendingDeliveries: 3,
      oldestReceivedAt: stalledSince.toISOString(),
    }),
  ])
})
