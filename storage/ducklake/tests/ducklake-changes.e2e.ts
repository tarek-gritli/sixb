import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { change, col, type DatasetRow, defineDataset } from "@sixb/core"
import type { DatasetRowChange } from "@sixb/core/lake-storage"
import { runLakeChangesContractSuite } from "@sixb/core/testing"
import type { DuckLakeStorage } from "../src"
import type { DuckDbExclusiveRuntime, DuckDbRuntime } from "../src/internal/duckdb-runtime"
import { encodeDatasetTableName } from "../src/internal/names"
import { quoteIdentifier, quoteSqlString } from "../src/internal/sql"
import { parseVersionId } from "../src/internal/versions"
import { collectRows, createLocalDuckLakeStorage } from "./test-utils"

const roots = new WeakMap<DuckLakeStorage, string>()
runLakeChangesContractSuite("DuckLakeStorage changes", {
  async createStorage() {
    const root = await mkdtemp(join(tmpdir(), "sixb-ducklake-changes-"))
    const storage = createLocalDuckLakeStorage(root)
    roots.set(storage, root)
    return storage
  },
  async teardown(storage) {
    await storage.close()
    await rm(roots.get(storage)!, { recursive: true, force: true })
  },
})

interface ChangeReaderInternals {
  readonly changes: {
    compareFeed(...args: unknown[]): Promise<boolean>
  }
  readonly connections: {
    attachedRuntime(): Promise<DuckDbRuntime>
    withExclusiveAttached<T>(run: (runtime: DuckDbExclusiveRuntime) => Promise<T>): Promise<T>
  }
}

describe("DuckLakeStorage changes from the change feed", () => {
  let rootDir: string
  let storage: DuckLakeStorage

  // Both versions together hold just over FEED_MIN_ROWS rows.
  const accounts = defineDataset("crm.accounts", {
    schema: [col("id", "string"), col("name", "string"), col("score", "int64")],
    primaryKey: "id",
  })
  const seedRows = Array.from({ length: 50_100 }, (_, i) => ({
    id: `a${i}`,
    name: "seed",
    score: i,
  }))

  beforeEach(async () => {
    rootDir = await mkdtemp(join(tmpdir(), "sixb-ducklake-feed-"))
    storage = createLocalDuckLakeStorage(rootDir)
    await storage.createDataset(accounts)
  })

  afterEach(async () => {
    await storage.close()
    await rm(rootDir, { recursive: true, force: true })
  })

  // Regression proof: in compareFeed, stop cancelling rows by row id. A key changed twice then
  // shows up as a change. Or in planFeedRead, read one range across the maintenance commit: it
  // then holds every row the compaction moved, and the read falls back to comparing in full.
  test("reads exactly what changed between any two versions, across merges and compaction", async () => {
    const versions: string[] = []
    let rows: readonly DatasetRow[] = seedRows
    versions.push((await snapshot(rows)).versionId)

    rows = edited(rows, { rename: ["a1", "a2", "a3"], remove: ["a5", "a6"], add: ["n1", "n2"] })
    versions.push((await snapshot(rows)).versionId)

    const merge = await storage.beginMerge({ dataset: accounts })
    await merge.writeChanges([
      change.upsert({ id: "a1", name: "merged", score: 1 }),
      change.upsert({ id: "n3", name: "merged", score: 3 }),
      change.delete({ id: "a7" }),
    ])
    const merged = await merge.commit()
    versions.push(merged.version!.versionId)
    rows = await currentRows()

    const append = await storage.beginWrite({ dataset: accounts, mode: "append" })
    await append.writeRows([{ id: "n4", name: "appended", score: 4 }])
    versions.push((await append.commit()).versionId)
    rows = await currentRows()

    // The same rows from new inputs: a version that changes none.
    const provenance = await storage.beginWrite({
      dataset: accounts,
      mode: "snapshot",
      inputs: [{ datasetId: "crm.sources", versionId: "ducklake:1" }],
    })
    await provenance.writeRows(rows)
    versions.push((await provenance.commit()).versionId)

    await compact()

    // a1 changes again, and n1, added in between, goes.
    rows = edited(rows, { rename: ["a1", "a4"], remove: ["n1"], add: ["n5"] })
    versions.push((await snapshot(rows)).versionId)
    expect(new Set(versions).size).toBe(versions.length)

    const compareFeed = spyOn(internals(storage).changes, "compareFeed")
    for (const [index, from] of versions.entries()) {
      for (const to of versions.slice(index + 1)) {
        const calls = compareFeed.mock.calls.length
        expect(await changesBetween(from, to)).toEqual(await expectedChanges(from, to))

        // Read from the feed, not compared in full after it.
        expect(compareFeed.mock.calls.length).toBe(calls + 1)
        expect(await compareFeed.mock.results.at(-1)?.value).toBe(true)
      }
    }
  }, 60_000)

  // Regression proof: in planFeedRead, drop the check that a version recorded rowChanges, or the
  // one that every snapshot in between is still there. The replacement, or the expired version,
  // then goes unread, and the delta misses its changes.
  test("compares the versions instead when the feed cannot tell", async () => {
    const compareFeed = spyOn(internals(storage).changes, "compareFeed")
    const first = (await snapshot(seedRows)).versionId

    // A full replacement in between: every row changes.
    const replaced = seedRows.map((row) => ({ ...row, score: row.score + 1 }))
    const replacement = (await snapshot(replaced)).versionId
    const restored = (await snapshot(seedRows)).versionId
    expect(await changesBetween(first, restored)).toEqual([])
    expect(await changesBetween(first, replacement)).toEqual(
      await expectedChanges(first, replacement)
    )

    // A version written outside Sixb, with no record of what it changed.
    await internals(storage).connections.withExclusiveAttached(async (runtime) => {
      await runtime.run(
        `UPDATE sixb_lake.main.${quoteIdentifier(encodeDatasetTableName(accounts.id))} SET name = 'outside' WHERE id = 'a9'`
      )
    })
    const outside = (await storage.getLatestVersion(accounts.id))!.versionId
    const after = (await snapshot(edited(await currentRows(), { rename: ["a10"] }))).versionId
    expect(await changesBetween(restored, after)).toEqual(await expectedChanges(restored, after))
    expect(outside).not.toBe(restored)

    // A version in between expired by retention.
    const middle = (await snapshot(edited(await currentRows(), { rename: ["a11"] }))).versionId
    const last = (await snapshot(edited(await currentRows(), { rename: ["a12"] }))).versionId
    const expected = await expectedChanges(after, last)
    await internals(storage).connections.withExclusiveAttached(async (runtime) => {
      await runtime.run(
        `CALL ducklake_expire_snapshots('sixb_lake', versions => [${parseVersionId(middle)}])`
      )
    })
    expect(await changesBetween(after, last)).toEqual(expected)

    // Identities other than the primary key are compared too.
    const byName = await storage.readChanges({
      datasetId: accounts.id,
      fromVersionId: first,
      toVersionId: last,
      keyColumns: ["id", "name"],
      columns: ["id", "name"],
    })
    await byName?.close()

    expect(compareFeed).not.toHaveBeenCalled()
  }, 60_000)

  // Regression proof: in compareVersions, count distinct keys whatever the versions validated.
  // The comparison, which a full replacement in between sends there, then runs the distinct count
  // on both versions.
  test("compares two large versions on their validated primary key without counting distinct keys", async () => {
    const first = (await snapshot(seedRows)).versionId
    await snapshot(seedRows.map((row) => ({ ...row, name: "replaced" })))
    const last = (await snapshot(edited(seedRows, { rename: ["a1"] }))).versionId

    const runtime = await internals(storage).connections.attachedRuntime()
    const query = spyOn(runtime, "query")
    try {
      expect(await changesBetween(first, last)).toEqual(await expectedChanges(first, last))
      expect(query.mock.calls.some(([sql]) => /count\(DISTINCT/i.test(String(sql)))).toBe(false)
    } finally {
      query.mockRestore()
    }

    // A blank key still refuses the change read, as for any identity.
    const blank = (await snapshot([...seedRows, { id: " ", name: "blank", score: 0 }])).versionId
    expect(
      await storage.readChanges({
        datasetId: accounts.id,
        fromVersionId: last,
        toVersionId: blank,
        keyColumns: ["id"],
        columns: ["id", "name"],
      })
    ).toBeNull()
  }, 60_000)

  async function snapshot(rows: readonly DatasetRow[]) {
    const write = await storage.beginWrite({ dataset: accounts, mode: "snapshot" })
    await write.writeRows(rows)
    return write.commit()
  }

  async function currentRows(versionId?: string) {
    const rows = await collectRows(storage.readRows({ datasetId: accounts.id, versionId }))
    return rows.map((row) => ({ ...row, score: Number(row.score) }))
  }

  async function changesBetween(fromVersionId: string, toVersionId: string) {
    const delta = await storage.readChanges({
      datasetId: accounts.id,
      fromVersionId,
      toVersionId,
      keyColumns: ["id"],
      columns: ["id", "name", "score"],
    })
    if (!delta) throw new Error("[Test] The change read fell back to a full read.")

    const changes: DatasetRowChange[] = []
    try {
      for await (const change of delta.changes) changes.push(change)
    } finally {
      await delta.close()
    }

    expect(delta.changeCount).toBe(changes.length)
    return sortChanges(changes)
  }

  /** The changes between two versions, from reading both in full. */
  async function expectedChanges(fromVersionId: string, toVersionId: string) {
    const read = async (versionId: string) =>
      new Map(
        (await collectRows(storage.readRows({ datasetId: accounts.id, versionId }))).map((row) => [
          String(row.id),
          row,
        ])
      )
    const before = await read(fromVersionId)
    const after = await read(toVersionId)

    const changes: DatasetRowChange[] = []
    for (const id of new Set([...before.keys(), ...after.keys()])) {
      const left = before.get(id) ?? null
      const right = after.get(id) ?? null
      if (JSON.stringify(left) !== JSON.stringify(right)) {
        changes.push({ before: left, after: right })
      }
    }

    return sortChanges(changes)
  }

  async function compact() {
    const table = quoteSqlString(encodeDatasetTableName(accounts.id))
    for (const calls of [
      [`CALL ducklake_flush_inlined_data('sixb_lake', table_name => ${table})`],
      [
        `CALL ducklake_rewrite_data_files('sixb_lake', ${table}, delete_threshold => 0)`,
        `CALL ducklake_merge_adjacent_files('sixb_lake', ${table})`,
      ],
    ]) {
      await internals(storage).connections.withExclusiveAttached(async (runtime) => {
        await runtime.run("BEGIN TRANSACTION")
        for (const call of calls) await runtime.run(call)
        await runtime.run("COMMIT")
      })
    }
  }
})

function internals(storage: DuckLakeStorage): ChangeReaderInternals {
  return storage as unknown as ChangeReaderInternals
}

function edited(
  rows: readonly DatasetRow[],
  edit: {
    readonly rename?: readonly string[]
    readonly remove?: readonly string[]
    readonly add?: readonly string[]
  }
): DatasetRow[] {
  const renamed = rows
    .filter((row) => !edit.remove?.includes(String(row.id)))
    .map((row) =>
      edit.rename?.includes(String(row.id)) ? { ...row, name: `${String(row.name)}+` } : row
    )
  return [...renamed, ...(edit.add ?? []).map((id) => ({ id, name: "added", score: 0 }))]
}

function sortChanges(changes: readonly DatasetRowChange[]): DatasetRowChange[] {
  const key = (change: DatasetRowChange) => String((change.after ?? change.before)?.id)
  return [...changes].sort((left, right) => key(left).localeCompare(key(right)))
}
