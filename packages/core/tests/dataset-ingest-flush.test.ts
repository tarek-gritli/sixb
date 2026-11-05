import { afterEach, expect, jest, test } from "bun:test"
import {
  change,
  col,
  type DatasetDefinition,
  type DatasetRow,
  defineDataset,
  events,
  InMemoryBlobStorage,
  InMemoryBroker,
  InMemoryLakeStorage,
  InMemoryQueues,
  InMemoryStorage,
  type MergeChange,
  type SixbErrorContext,
  SixbHost,
} from "../src"
import { writeDataset } from "../src/datasets/write"
import { flushSixbErrors } from "../src/error-reporting/capability"
import { buildEventSelectorPredicate } from "../src/events"
import { createTestingScope } from "../src/execution/scopes"
import { DatasetIngestFlushError, getDatasetIngestRuntime } from "../src/runtime/internal"

const people = defineDataset("source.people", {
  schema: [col("id", "string"), col("revision", "int64"), col("name", "string")],
  primaryKey: "id",
  sequenceBy: "revision",
})

const contacts = defineDataset("source.contacts", {
  schema: [col("id", "string"), col("name", "string")],
  primaryKey: "id",
})

const documents = defineDataset("source.documents", {
  schema: [col("id", "string"), col("file", "fileRef")],
  primaryKey: "id",
})

afterEach(() => {
  jest.useRealTimers()
})

function setup(lakeStorage = new InMemoryLakeStorage()) {
  const storage = new InMemoryStorage()
  const blobStorage = new InMemoryBlobStorage()
  const reports: SixbErrorContext[] = []
  const host = new SixbHost({
    id: "ingest-flush-test",
    ontology: [],
    datasets: [people, contacts, documents],
    lakeStorage,
    broker: new InMemoryBroker(),
    storage,
    queues: new InMemoryQueues(),
    blobStorage,
    onError: (_error, context) => {
      reports.push(context)
    },
  })

  const scope = () => host.withScope(createTestingScope({ projectId: host.id }))
  const sixb = scope()
  const flush = (dataset: DatasetDefinition = people) =>
    getDatasetIngestRuntime(host).flush(dataset.id, new AbortController().signal)
  const ingest = (dataset: DatasetDefinition, ...changes: MergeChange<DatasetRow, DatasetRow>[]) =>
    sixb.datasets.ingest(dataset, { changes })
  const committed = async () =>
    (await host.events.read()).filter((event) => event.type === "dataset.version.committed")
  const rows = async (dataset: DatasetDefinition = people) => {
    const read = await sixb.datasets.readRows(dataset)
    const all: DatasetRow[] = []
    for await (const row of read?.rows ?? []) all.push(row)
    return all.sort((left, right) => String(left.id).localeCompare(String(right.id)))
  }
  const reported = async () => {
    await flushSixbErrors(host)
    return reports
  }

  return {
    host,
    storage,
    lakeStorage,
    blobStorage,
    sixb,
    scope,
    flush,
    ingest,
    committed,
    rows,
    reported,
  }
}

test("a flush writes every waiting delivery as one version, in delivery order", async () => {
  // Regression proof: read the batch's deliveries newest first in `values`; without a source
  // sequence, "1" then ends on the first delivery's value.
  const { host, lakeStorage, scope, flush, committed, rows, storage } = setup()
  const first = scope()
  const second = scope()

  await first.datasets.ingest(contacts, {
    changes: [change.upsert({ id: "1", name: "Sam" }), change.upsert({ id: "7", name: "Gone" })],
  })
  await second.datasets.ingest(contacts, {
    changes: [
      change.upsert({ id: "1", name: "Samuel" }),
      change.upsert({ id: "2", name: "Ada" }),
      change.delete({ id: "7" }),
    ],
  })

  expect(await flush(contacts)).toEqual({ kind: "flushed", versions: 1, deliveries: 2 })

  const versions = await lakeStorage.listVersions(contacts.id)
  expect(versions).toHaveLength(1)
  expect(versions[0]?.producer).toEqual({
    kind: "ingest",
    id: first.execution.id,
    runId: expect.stringMatching(/^flush_/),
  })
  expect((await rows(contacts)).map((row) => row.name)).toEqual(["Samuel", "Ada"])

  expect(await committed()).toMatchObject([
    {
      correlationId: first.execution.correlationId,
      payload: { datasetId: contacts.id, versionId: versions[0]?.versionId, rowsChanged: true },
    },
  ])
  expect(await storage.datasetIngest.summarize({ projectId: host.id })).toEqual([])

  // An empty inbox writes nothing.
  expect(await flush(contacts)).toEqual({ kind: "flushed", versions: 0, deliveries: 0 })
  expect(await lakeStorage.listVersions(contacts.id)).toHaveLength(1)
})

test("one flush gives the rows the deliveries give applied one by one", async () => {
  // Regression proof: in `values`, yield each delivery's changes in reverse; the rows differ.
  for (const dataset of [people, contacts]) {
    const oneByOne = new InMemoryLakeStorage()
    const { ingest, flush, rows } = setup()

    for (const delivery of scriptedDeliveries(dataset)) {
      await ingest(dataset, ...delivery)
      await writeDataset({
        lakeStorage: oneByOne,
        blobStorage: new InMemoryBlobStorage(),
        dataset,
        mode: "merge",
        signal: new AbortController().signal,
        readValues: async () => delivery.map((value) => ({ value })),
      })
    }

    await flush(dataset)

    const expected =
      (await oneByOne.listVersions(dataset.id)).length > 0 ? await readAll(oneByOne, dataset) : []
    expect(await rows(dataset)).toEqual(expected)
  }
})

test("a version says whether the flush changed rows or lineage", async () => {
  // Red check: emit `rowsChanged: true` instead of the created version's flag.
  const { ingest, flush, committed } = setup()
  const updated = buildEventSelectorPredicate(events.dataset(people).updated())

  await ingest(people, change.upsert({ id: "42", revision: 8, name: "Sam" }))
  await flush()

  // The delete of a person the dataset never held only advances source ordering.
  await ingest(people, change.delete({ id: "7" }, { sequence: 3 }))
  await flush()

  // A row that only carries a newer sequence triggers no dataset update.
  await ingest(people, change.upsert({ id: "42", revision: 9, name: "Sam" }))
  await flush()

  const announced = await committed()
  expect(
    announced.map((event) => [event.payload.rowsChanged, event.payload.lineageChanged])
  ).toEqual([
    [true, true],
    [false, false],
    [false, false],
  ])
  expect(announced.map(updated)).toEqual([true, false, false])
})

test("a delivery the lake refuses is reported with what to send again, and the rest commits", async () => {
  // Regression proof: return null from `findConflicting`; the flush fails and commits nothing.
  const { scope, ingest, flush, rows, reported } = setup()
  await ingest(people, change.upsert({ id: "42", revision: 8, name: "Sam" }))
  await flush()

  const kept = scope()
  const refusedByLake = scope()
  const refusedByBatch = scope()
  await kept.datasets.ingest(people, {
    changes: [
      change.upsert({ id: "43", revision: 1, name: "Ada" }),
      change.upsert({ id: "44", revision: 1, name: "Grace" }),
    ],
  })
  // Sequence 8 of "42" is in the lake with other content.
  const againstLake = await refusedByLake.datasets.ingest(people, {
    changes: [change.upsert({ id: "42", revision: 8, name: "Conflict" })],
  })
  // Sequence 1 of "44" is in this batch with other content: the later delivery is refused.
  const againstBatch = await refusedByBatch.datasets.ingest(people, {
    changes: [
      change.upsert({ id: "45", revision: 1, name: "Linus" }),
      change.upsert({ id: "44", revision: 1, name: "Other" }),
    ],
  })

  expect(await flush()).toEqual({ kind: "flushed", versions: 1, deliveries: 1 })
  expect((await rows()).map((row) => row.name)).toEqual(["Sam", "Ada", "Grace"])

  const rejected = (await reported()).filter((report) => report.type === "dataset.ingest.rejected")
  expect(rejected).toEqual([
    expect.objectContaining({
      datasetId: people.id,
      deliveryId: againstLake.deliveryId,
      executionId: refusedByLake.execution.id,
      changeIndex: 0,
      key: { id: "42" },
      sequence: "8",
      failure: expect.objectContaining({ code: "dataset.change_refused" }),
    }),
    expect.objectContaining({
      deliveryId: againstBatch.deliveryId,
      executionId: refusedByBatch.execution.id,
      changeIndex: 1,
      key: { id: "44" },
      sequence: "1",
    }),
  ])
})

test("a change that stopped being valid since ingest is refused, and the rest commits", async () => {
  // Regression proof: refuse nothing in `mapValidationError` (return the error as it is); the
  // flush fails on the deleted blob and writes nothing.
  const { blobStorage, scope, flush, rows, reported } = setup()
  const kept = await blobStorage.put({ body: new TextEncoder().encode("kept") })
  const deleted = await blobStorage.put({ body: new TextEncoder().encode("deleted") })

  const sender = scope()
  await sender.datasets.ingest(documents, { changes: [change.upsert({ id: "1", file: kept })] })
  const refused = await sender.datasets.ingest(documents, {
    changes: [change.upsert({ id: "2", file: kept }), change.upsert({ id: "3", file: deleted })],
  })

  // The blob is deleted between ingest and flush.
  const stat = blobStorage.stat.bind(blobStorage)
  blobStorage.stat = async (blobId) => (blobId === deleted.blobId ? null : stat(blobId))

  expect(await flush(documents)).toEqual({ kind: "flushed", versions: 1, deliveries: 1 })
  expect((await rows(documents)).map((row) => row.id)).toEqual(["1"])

  const rejected = (await reported()).filter((report) => report.type === "dataset.ingest.rejected")
  expect(rejected).toEqual([
    expect.objectContaining({
      deliveryId: refused.deliveryId,
      executionId: sender.execution.id,
      changeIndex: 1,
      key: { id: "3" },
      failure: expect.objectContaining({ code: "dataset.change_refused" }),
    }),
  ])
})

test("a blob storage error at flush is retried, never refused", async () => {
  // Regression proof: refuse every error in `mapValidationError`; the delivery is dropped and
  // reported as refused although its blob exists.
  const { blobStorage, ingest, flush, rows, reported, storage, host } = setup()
  const file = await blobStorage.put({ body: new TextEncoder().encode("hello") })
  await ingest(documents, change.upsert({ id: "1", file }))

  const stat = blobStorage.stat.bind(blobStorage)
  blobStorage.stat = async () => {
    throw new Error("ECONNRESET")
  }
  const failed = await flush(documents).catch((error) => error)
  blobStorage.stat = stat

  expect(failed).toBeInstanceOf(DatasetIngestFlushError)
  expect(await storage.datasetIngest.summarize({ projectId: host.id })).toMatchObject([
    { datasetId: documents.id, pendingDeliveries: 1 },
  ])

  expect(await flush(documents)).toEqual({ kind: "flushed", versions: 1, deliveries: 1 })
  expect((await rows(documents)).map((row) => row.id)).toEqual(["1"])
  expect((await reported()).filter((report) => report.type === "dataset.ingest.rejected")).toEqual(
    []
  )
})

test("a flush that dies after its commit is resumed without writing its batch again", async () => {
  // Regression proof: start a resumed batch as a new one (skip `findCommitted` on resume); the
  // batch is written again over the sync's newer value, as a third version.
  const { storage, lakeStorage, ingest, flush, committed, rows } = setup()
  await ingest(contacts, change.upsert({ id: "1", name: "From the webhook" }))

  const complete = storage.datasetIngest.complete
  storage.datasetIngest.complete = async () => {
    throw new Error("worker stopped")
  }
  const died = await flush(contacts).catch((error) => error)
  storage.datasetIngest.complete = complete

  expect(died).toBeInstanceOf(DatasetIngestFlushError)
  expect(await lakeStorage.listVersions(contacts.id)).toHaveLength(1)

  // Before the batch is resumed, a sync writes a newer value.
  await writeDataset({
    lakeStorage,
    blobStorage: new InMemoryBlobStorage(),
    dataset: contacts,
    mode: "merge",
    signal: new AbortController().signal,
    readValues: async () => [{ value: change.upsert({ id: "1", name: "From the sync" }) }],
  })

  expect(await flush(contacts)).toEqual({ kind: "flushed", versions: 1, deliveries: 1 })
  expect(await lakeStorage.listVersions(contacts.id)).toHaveLength(2)
  expect((await rows(contacts)).map((row) => row.name)).toEqual(["From the sync"])

  // The resumed flush announces the version the dead one could not.
  expect(await committed()).toHaveLength(1)
})

test("a flush whose lease expired commits nothing twice when it wakes", async () => {
  // Regression proof: in `commit`, rethrow a LakeConcurrencyError without looking for the batch;
  // the woken flush then fails as if the lake had refused it, instead of finding it committed.
  jest.useFakeTimers()
  const lakeStorage = new InMemoryLakeStorage()
  const { storage, ingest, flush } = setup(lakeStorage)
  await ingest(contacts, change.upsert({ id: "1", name: "Ada" }))

  const paused = Promise.withResolvers<void>()
  const wake = Promise.withResolvers<void>()
  const beginMerge = lakeStorage.beginMerge.bind(lakeStorage)
  let calls = 0
  lakeStorage.beginMerge = async (input) => {
    const session = await beginMerge(input)
    calls += 1
    if (calls === 1) {
      paused.resolve()
      await wake.promise
    }
    return session
  }

  const sleeper = flush(contacts).catch((error) => error)
  await paused.promise

  // Its lease expires; another flush resumes the batch and commits it.
  jest.setSystemTime(new Date(Date.now() + 61_000))
  expect(await flush(contacts)).toEqual({ kind: "flushed", versions: 1, deliveries: 1 })

  wake.resolve()
  const woken = await sleeper

  expect(woken).toBeInstanceOf(Error)
  expect((woken as Error).name).toBe("DatasetIngestLeaseLostError")
  expect(await lakeStorage.listVersions(contacts.id)).toHaveLength(1)

  // The woken flush counted no failure: the next delivery finds a clean streak.
  await ingest(contacts, change.upsert({ id: "2", name: "Grace" }))
  expect(await storage.datasetIngest.summarize({ projectId: "ingest-flush-test" })).toMatchObject([
    { pendingDeliveries: 1, failureStreak: 0 },
  ])
})

test("a dataset whose flushes keep failing is reported once, at the fifth failure in a row", async () => {
  // Regression proof: report at every failure; six reports arrive.
  const lakeStorage = new InMemoryLakeStorage()
  const { ingest, flush, reported } = setup(lakeStorage)
  await ingest(people, change.upsert({ id: "42", revision: 8, name: "Sam" }))

  lakeStorage.beginMerge = async () => {
    throw new Error("lake offline")
  }

  const streaks: number[] = []
  for (let attempt = 0; attempt < 6; attempt += 1) {
    const failed = await flush().catch((error) => error)
    streaks.push((failed as DatasetIngestFlushError).failureStreak)
  }
  expect(streaks).toEqual([1, 2, 3, 4, 5, 6])

  const failures = (await reported()).filter(
    (report) => report.type === "dataset.ingest.flush.failed"
  )
  expect(failures).toEqual([
    expect.objectContaining({
      datasetId: people.id,
      failureStreak: 5,
      pendingDeliveries: 1,
      failure: expect.objectContaining({ code: "internal.unexpected" }),
    }),
  ])
})

test("a flush that spends its budget asks for the next job and leaves the rest", async () => {
  // Regression proof: drop `requestAgain` when the budget is spent; the second delivery then
  // waits with no request open.
  jest.useFakeTimers()
  const lakeStorage = new InMemoryLakeStorage()
  const { host, storage, ingest, flush } = setup(lakeStorage)

  // Two deliveries that each fill a batch.
  const batchOf = (from: number) =>
    Array.from({ length: 10_000 }, (_, index) =>
      change.upsert({ id: `${from + index}`, name: "N" })
    )
  await ingest(contacts, ...batchOf(0))
  await ingest(contacts, ...batchOf(10_000))

  // The first batch takes the whole budget.
  const beginMerge = lakeStorage.beginMerge.bind(lakeStorage)
  lakeStorage.beginMerge = async (input) => {
    jest.setSystemTime(new Date(Date.now() + 31_000))
    return beginMerge(input)
  }

  expect(await flush(contacts)).toEqual({ kind: "flushed", versions: 1, deliveries: 1 })
  expect(await storage.datasetIngest.summarize({ projectId: host.id })).toMatchObject([
    { datasetId: contacts.id, pendingDeliveries: 1, leaseExpiresAt: null },
  ])

  const queued = (await host.events.read()).filter(
    (event) => event.type === "dataset.ingest.queued"
  )
  expect(queued.map((event) => event.payload)).toEqual([
    { datasetId: contacts.id, request: 0 },
    { datasetId: contacts.id, request: 1 },
  ])
})

test("a second flush of the dataset waits for the first", async () => {
  const lakeStorage = new InMemoryLakeStorage()
  const { ingest, flush } = setup(lakeStorage)
  await ingest(contacts, change.upsert({ id: "1", name: "Ada" }))

  const paused = Promise.withResolvers<void>()
  const resume = Promise.withResolvers<void>()
  const beginMerge = lakeStorage.beginMerge.bind(lakeStorage)
  lakeStorage.beginMerge = async (input) => {
    paused.resolve()
    await resume.promise
    return beginMerge(input)
  }

  const first = flush(contacts)
  await paused.promise

  const second = await flush(contacts)
  expect(second.kind).toBe("busy")

  resume.resolve()
  expect(await first).toEqual({ kind: "flushed", versions: 1, deliveries: 1 })
})

/** A fixed mix of upserts and deletes over three keys, some of them out of source order. */
function scriptedDeliveries(dataset: DatasetDefinition): MergeChange<DatasetRow, DatasetRow>[][] {
  const sequenced = dataset.sequenceBy !== undefined
  let seed = 7
  const next = (modulo: number) => {
    seed = (seed * 48_271) % 2_147_483_647
    return seed % modulo
  }

  // Unique revisions, shuffled: some changes arrive after newer ones, none tie.
  const revisions = Array.from({ length: 30 }, (_, index) => index + 1)
  for (let index = revisions.length - 1; index > 0; index -= 1) {
    const swap = next(index + 1)
    ;[revisions[index], revisions[swap]] = [revisions[swap]!, revisions[index]!]
  }

  const deliveries: MergeChange<DatasetRow, DatasetRow>[][] = []
  let used = 0
  while (used < revisions.length) {
    const size = Math.min(1 + next(3), revisions.length - used)
    const delivery = revisions.slice(used, used + size).map((revision) => {
      const id = `k${next(3)}`
      if (next(4) === 0) {
        return sequenced ? change.delete({ id }, { sequence: revision }) : change.delete({ id })
      }
      return change.upsert(
        sequenced ? { id, revision, name: `r${revision}` } : { id, name: `r${revision}` }
      )
    })
    deliveries.push(delivery)
    used += size
  }

  return deliveries
}

async function readAll(lakeStorage: InMemoryLakeStorage, dataset: DatasetDefinition) {
  const version = await lakeStorage.getLatestVersion(dataset.id)
  if (!version) return []

  const all: DatasetRow[] = []
  for await (const row of lakeStorage.readRows({
    datasetId: dataset.id,
    versionId: version.versionId,
  })) {
    all.push(row)
  }
  return all.sort((left, right) => String(left.id).localeCompare(String(right.id)))
}
