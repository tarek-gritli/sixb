import { reportLakeRetentionFailure } from "@sixb/core/internal/error-reporting"
import { captureSixbFailure } from "@sixb/core/internal/errors"
import {
  DatasetIngestFlushError,
  type DatasetIngestRuntime,
  getDatasetIngestRuntime,
  getLakeRetentionRuntime,
  type LakeRetentionRuntime,
} from "@sixb/core/internal/runtime"
import { QueueWorker, type QueueWorkerFailureDecision } from "@sixb/core/internal/workers"
import type { LakeRetentionReport } from "@sixb/core/lake-storage"
import type {
  ClaimedQueueJob,
  DatasetIngestFlushRequestedQueueJob,
  LakeQueueJob,
  LakeRetentionRequestedQueueJob,
  Queues,
} from "@sixb/core/queues"

const FAILURE_CODES = ["internal.unexpected", "runtime.cancelled"] as const

/**
 * Four at a time, so flushes of several datasets run together and beside a retention pass, which
 * can take minutes. Retention takes at most one of them.
 */
const LAKE_CONCURRENCY = 4

/** A failed retention run is tried this many times in all, a minute apart, before it is reported. */
const RETENTION_MAX_ATTEMPTS = 3
const RETENTION_RETRY_DELAY_MS = 60_000

/**
 * A failed flush is tried again sooner at first, then every minute, for as long as it fails: its
 * changes stay in the inbox until one succeeds. The flush reports a dataset that keeps failing.
 */
const FLUSH_RETRY_DELAYS_MS = [1_000, 2_000, 5_000, 15_000, 30_000, 60_000] as const

export interface LakeWorkerHost {
  readonly id: string
  readonly queues: Pick<Queues, "lake">
}

/** Another flush holds the dataset: this job waits until its lease would expire. */
class DatasetIngestFlushBusy extends Error {
  constructor(readonly until: Date) {
    super("[SixbSyncWorker] Another flush holds the dataset.")
  }
}

/** Separate lane: lake work never takes a sync slot, and a sync never delays it. */
export class LakeWorker extends QueueWorker<LakeQueueJob, typeof FAILURE_CODES> {
  private readonly host: LakeWorkerHost
  private readonly retention: LakeRetentionRuntime
  private readonly ingest: DatasetIngestRuntime
  // Retention passes run one after another, so they never hold every slot.
  private retentionTail: Promise<unknown> = Promise.resolve()

  constructor(host: LakeWorkerHost) {
    super({
      projectId: host.id,
      queue: host.queues.lake,
      failureCodes: FAILURE_CODES,
      workerId: `lake-worker-${host.id}`,
      claimLimit: LAKE_CONCURRENCY,
    })

    this.host = host
    this.retention = getLakeRetentionRuntime(host)
    this.ingest = getDatasetIngestRuntime(host)
  }

  protected async execute(claimed: ClaimedQueueJob<LakeQueueJob>, signal: AbortSignal) {
    const { job } = claimed

    switch (job.type) {
      case "lake.retention.requested":
        await this.runRetention(job, signal)
        return
      case "dataset.ingest.flush.requested":
        await this.flush(job, signal)
        return
    }
  }

  private async runRetention(job: LakeRetentionRequestedQueueJob, signal: AbortSignal) {
    const run = this.retentionTail.then(() =>
      this.retention.runOccurrence(job.payload.occurrenceAt, signal)
    )
    this.retentionTail = run.catch(() => undefined)

    const report = await run
    if (report && changedAnything(report)) {
      console.info(
        `[SixbSyncWorker] Lake retention expired ${report.expiredVersions} version(s) and deleted ` +
          `${report.deletedFiles + report.orphanedFiles} file(s)` +
          (report.hasMore ? "; the next run continues." : ".")
      )
    }
  }

  private async flush(job: DatasetIngestFlushRequestedQueueJob, signal: AbortSignal) {
    const outcome = await this.ingest.flush(job.payload.datasetId, signal)
    if (outcome.kind === "busy") throw new DatasetIngestFlushBusy(outcome.until)
  }

  protected override onExecutionError(
    claimed: ClaimedQueueJob<LakeQueueJob>,
    error: unknown
  ): QueueWorkerFailureDecision<(typeof FAILURE_CODES)[number]> {
    const { job } = claimed

    if (job.type === "dataset.ingest.flush.requested") {
      return this.retryFlush(job, error)
    }

    if (job.attempt < RETENTION_MAX_ATTEMPTS) {
      console.warn(
        `[SixbSyncWorker] Lake retention failed (attempt ${job.attempt} of ${RETENTION_MAX_ATTEMPTS}); retrying in a minute.`,
        error
      )
      return { kind: "retry", availableAt: inMs(RETENTION_RETRY_DELAY_MS) }
    }

    const failure = captureSixbFailure(error, {
      allowedCodes: FAILURE_CODES,
      defaultCode: "internal.unexpected",
    })
    reportLakeRetentionFailure(this.host, error, {
      projectId: this.host.id,
      occurrenceAt: job.payload.occurrenceAt,
      attempt: job.attempt,
      failure,
    })

    return { kind: "fail", failure }
  }

  /** A flush job is never failed: the next attempt resumes the batch where this one stopped. */
  private retryFlush(
    job: DatasetIngestFlushRequestedQueueJob,
    error: unknown
  ): QueueWorkerFailureDecision<(typeof FAILURE_CODES)[number]> {
    if (error instanceof DatasetIngestFlushBusy) {
      return { kind: "retry", availableAt: error.until.toISOString() }
    }

    const failures = error instanceof DatasetIngestFlushError ? error.failureStreak : job.attempt
    const delayMs =
      FLUSH_RETRY_DELAYS_MS[Math.min(Math.max(failures, 1), FLUSH_RETRY_DELAYS_MS.length) - 1] ??
      FLUSH_RETRY_DELAYS_MS[0]

    console.warn(
      `[SixbSyncWorker] Flushing the ingested changes of dataset '${job.payload.datasetId}' failed; retrying in ${delayMs / 1_000} s.`,
      error
    )
    return { kind: "retry", availableAt: inMs(delayMs) }
  }
}

function inMs(delayMs: number): string {
  return new Date(Date.now() + delayMs).toISOString()
}

function changedAnything(report: LakeRetentionReport): boolean {
  return report.expiredVersions + report.deletedFiles + report.orphanedFiles > 0
}
