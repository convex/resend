/// <reference types="vite/client" />

import { AsyncLocalStorage } from "node:async_hooks";
import { version } from "convex";
import {
  ActionBuilder,
  ApiFromModules,
  DataModelFromSchemaDefinition,
  DefaultFunctionArgs,
  DocumentByName,
  FilterApi,
  FunctionReference,
  FunctionReference_future,
  FunctionReturnType,
  GenericActionCtx,
  GenericDataModel,
  GenericDocument,
  GenericMutationCtx,
  GenericQueryCtx,
  GenericSchema,
  HttpRouter,
  MutationBuilder,
  OptionalRestArgs,
  PublicHttpAction,
  QueryBuilder,
  RegisteredAction,
  RegisteredMutation,
  RegisteredQuery,
  RequestMetadata,
  SchemaDefinition,
  SystemDataModel,
  UserIdentity,
  actionGeneric,
  anyApi,
  componentsGeneric,
  filterApi,
  getFunctionAddress,
  httpActionGeneric,
  internalActionGeneric,
  internalMutationGeneric,
  internalQueryGeneric,
  makeFunctionReference,
  mutationGeneric,
  queryGeneric,
} from "convex/server";
import {
  Base64,
  CommitTsPlaceholder,
  GenericId,
  JSONValue,
  Value,
  compareValues,
  convexToJson,
  getConvexSize,
  getDocumentSize,
  jsonToConvex,
} from "convex/values";
import {
  TransactionMetricsTracker,
  TransactionMetrics,
} from "./transactionMetrics.js";

// Real Convex IDs are 32 characters. IDs may exceed this length for very long
// table names or large numbers of documents, but this is the typical target.
const TYPICAL_CONVEX_ID_LENGTH = 32;
const MAX_SEARCH_CANDIDATE_REVISIONS = 1024;

// Matches MAX_USER_SIZE in the Convex backend: a document, including its
// system fields, may be at most 1 MiB.
const MAX_DOCUMENT_SIZE = 1 << 20;

function checkDocumentSize(tableName: string, document: StoredDocument) {
  // System tables such as `_scheduled_functions` have their own limits.
  if (tableName.startsWith("_")) {
    return;
  }
  const size = getDocumentSize(document);
  if (size > MAX_DOCUMENT_SIZE) {
    throw new Error(
      `Value is too large (${size} bytes > maximum size ${MAX_DOCUMENT_SIZE} bytes)`,
    );
  }
}

type FilterJson =
  { $eq: [FilterJson, FilterJson] } | { $field: string } | JSONValue;

type QueryOperator = { filter: FilterJson } | { limit: number };
type Source =
  | { type: "FullTableScan"; tableName: string; order: "asc" | "desc" | null }
  | {
      type: "IndexRange";
      indexName: string;
      range: ReadonlyArray<SerializedRangeExpression>;
      order: "asc" | "desc" | null;
    }
  | {
      type: "Search";
      indexName: string;
      filters: ReadonlyArray<SerializedSearchFilter>;
    };

type SerializedQuery = {
  source: Source;
  operators: Array<QueryOperator>;
};

type SerializedRangeExpression = {
  type: "Eq" | "Gt" | "Gte" | "Lt" | "Lte";
  fieldPath: string;
  value: JSONValue;
};

type SerializedSearchFilter =
  | {
      type: "Search";
      fieldPath: string;
      value: string;
    }
  | {
      type: "Eq";
      fieldPath: string;
      value: JSONValue;
    };

type ScheduledFunction = DocumentByName<
  SystemDataModel,
  "_scheduled_functions"
>;

type StoredDocument = GenericDocument & {
  _id: DocumentId;
  _creationTime: number;
};

type Index = {
  indexDescriptor: string;
  fields: string[];
};

type VectorIndex = {
  indexDescriptor: string;
  vectorField: string;
  dimensions: number;
  filterFields: string[];
};

type QueryId = number;

type TableName = string;

type DocumentId = GenericId<TableName>;

const ROOT_COMPONENT_PATH = "";

class NestedLock {
  private _lock: Promise<void> | null = null;
  private _release: (() => void) | null = null;
  transactionActive = true;

  constructor(
    readonly transactionManager: TransactionManager,
    readonly parent: NestedLock | undefined,
  ) {}

  async acquire() {
    while (this._lock !== null) {
      await this._lock;
    }
    this._lock = new Promise((resolve) => {
      this._release = resolve;
    });
  }

  release() {
    this._lock = null;
    this._release!();
    this._release = null;
  }
}

const nestedTxStorage = new AsyncLocalStorage<NestedLock>();
const authStorage = new AsyncLocalStorage<AuthFake>();
const convexGlobalStorage = new AsyncLocalStorage<ConvexGlobal>();

class DatabaseFake {
  private _componentPath: string;
  private _documents: Record<DocumentId, StoredDocument> = {};
  // Ids of committed documents, grouped by table name, so queries only
  // visit documents in the table they read from. Kept in sync with
  // `_documents` in `commit`.
  private _tableIds: Map<string, Set<DocumentId>> = new Map();
  private _storage: Record<DocumentId, Blob> = {};
  private _nextQueryId: QueryId = 1;
  private _nextDocId: number = 10000;
  private _lastCreationTime: number = 0;
  private _queryResults: Record<QueryId, Iterator<GenericDocument>> = {};
  // Pending writes for each level of transaction nesting.
  // Whenever a child mutation begins, a new level is created for all
  // components.
  // - When a mutation performs updates, they are staged in the last
  //   level of writes for the mutation's component.
  // - When a child mutation commits, the writes are merged up one level.
  // - When the top-level mutation commits, the writes are applied to the
  //   database.
  // - When a mutation is rolled back, last level of writes is discarded.
  private _writes: Array<Record<DocumentId, StoredDocument | null>> = [];
  // Files stored at each level of `_writes`, staged the same way so a
  // rolled-back mutation's files are discarded along with their `_storage`
  // documents.
  private _pendingFiles: Array<Map<DocumentId, Blob>> = [];

  private _schema: {
    schemaValidation: boolean;
    tables: Map<
      string,
      {
        indexes: Index[];
        vectorIndexes: VectorIndex[];
        documentType: ValidatorJSON;
      }
    >;
  } | null;

  constructor(
    schema: SchemaDefinition<GenericSchema, boolean> | null,
    componentPath: string,
  ) {
    this._schema =
      schema === null
        ? null
        : {
            schemaValidation: (schema as any).schemaValidation,
            tables: new Map(
              Object.entries(schema.tables).map(([name, tableSchema]) => [
                name,
                (tableSchema as any).export(),
              ]),
            ),
          };
    this._componentPath = componentPath;

    this.validateSchema();
  }

  // Called by TransactionManager.begin.
  startTransaction() {
    this._writes.push({});
    this._pendingFiles.push(new Map());
  }

  get<Table extends TableName>(
    tableName: Table | undefined,
    id: GenericId<Table>,
  ) {
    this._validateId(tableName, id);

    for (let i = this._writes.length - 1; i >= 0; i--) {
      const write = this._writes[i][id];
      if (write !== undefined) {
        return write;
      }
    }

    return this._documents[id] ?? null;
  }

  // Note that this is not the format the real backend
  // uses for IDs.
  private _generateId<TableName extends string>(
    table: TableName,
  ): GenericId<TableName> {
    const counterPadLen = TYPICAL_CONVEX_ID_LENGTH - table.length;
    const counter = this._nextDocId.toString().padStart(counterPadLen, "0");
    const id = counter + table;
    this._nextDocId += 1;
    return id as GenericId<TableName>;
  }

  storeFile(storageId: GenericId<"_storage">, blob: Blob) {
    if (this._pendingFiles.length === 0) {
      throw new Error(`File stored outside of transaction ${storageId}`);
    }
    this._pendingFiles[this._pendingFiles.length - 1].set(storageId, blob);
  }

  getFile(storageId: GenericId<"_storage">) {
    if (this.get("_storage", storageId) === null) {
      return null;
    }
    return this._pendingFile(storageId) ?? this._storage[storageId];
  }

  // Whether the file was stored by the current transaction.
  isPendingFile(storageId: GenericId<"_storage">) {
    return this._pendingFile(storageId) !== undefined;
  }

  private _pendingFile(storageId: GenericId<"_storage">) {
    for (let i = this._pendingFiles.length - 1; i >= 0; i--) {
      const file = this._pendingFiles[i].get(storageId);
      if (file !== undefined) {
        return file;
      }
    }
    return undefined;
  }

  private _addWrite(id: DocumentId, newValue: StoredDocument | null) {
    if (this._writes.length === 0) {
      throw new Error(`Write outside of transaction ${id}`);
    }
    this._writes[this._writes.length - 1][id] = newValue;
  }

  insert<Table extends TableName>(table: Table, value: any) {
    this._validate(table, value);
    const _id = this._generateId(table);
    const now = Date.now();
    const _creationTime =
      now <= this._lastCreationTime ? this._lastCreationTime + 0.001 : now;
    const document = { ...value, _id, _creationTime };
    checkDocumentSize(table, document);
    this._lastCreationTime = _creationTime;
    this._addWrite(_id, document);
    return _id;
  }

  patch<Table extends TableName>(
    tableName: Table | undefined,
    id: GenericId<Table>,
    value: Record<string, any>,
  ) {
    this._validateId(tableName, id);

    if (typeof value !== "object") {
      throw new Error(
        `Invalid argument \`value\` in \`db.patch\`, expected object but got '${typeof value}': ${value as any}`,
      );
    }

    const document = this.get(tableName, id);
    if (document === null) {
      throw new Error(`Patch on non-existent document with ID "${id}"`);
    }
    const { _id, _creationTime, ...fields } = document;
    if (value._id !== undefined && value._id !== _id) {
      throw new Error(
        `Provided \`_id\` field value "${value._id}" ` +
          `does not match the document ID "${_id}"`,
      );
    }
    if (
      value._creationTime !== undefined &&
      value._creationTime !== _creationTime
    ) {
      throw new Error(
        `Provided \`_creationTime\` field value ${value._creationTime} ` +
          `does not match the document's creation time ${_creationTime}`,
      );
    }
    delete value["_id"];
    delete value["_creationTime"];
    const convexValue: any = {};
    for (const [key, v] of Object.entries(value)) {
      convexValue[key] = evaluateValue(v);
    }
    const merged = { ...fields, ...convexValue };
    const table = tableNameFromId(_id)!;
    this._validate(table, merged);
    const patched = { _id, _creationTime, ...merged };
    checkDocumentSize(table, patched);
    this._addWrite(id, patched);
  }

  replace<Table extends TableName>(
    tableName: Table | undefined,
    id: DocumentId,
    value: Record<string, any>,
  ) {
    this._validateId(tableName, id);

    if (typeof value !== "object") {
      throw new Error(
        `Invalid argument \`value\` in \`db.replace\`, expected object but got '${typeof value}': ${value as any}`,
      );
    }

    const document = this.get(tableName, id);
    if (document === null) {
      throw new Error(`Replace on non-existent document with ID "${id}"`);
    }
    if (value._id !== undefined && value._id !== document._id) {
      throw new Error(
        `Provided \`_id\` field value "${value._id}" ` +
          `does not match the document ID "${document._id}"`,
      );
    }
    if (
      value._creationTime !== undefined &&
      value._creationTime !== document._creationTime
    ) {
      throw new Error(
        `Provided \`_creationTime\` field value ${value._creationTime} ` +
          `does not match the document's creation time ${document._creationTime}`,
      );
    }
    delete value["_id"];
    delete value["_creationTime"];
    const convexValue: any = {};
    for (const [key, v] of Object.entries(value)) {
      convexValue[key] = evaluateValue(v);
    }
    const table = tableNameFromId(document._id)!;
    this._validate(table, convexValue);
    const replacement = {
      ...convexValue,
      _id: document._id,
      _creationTime: document._creationTime,
    };
    checkDocumentSize(table, replacement);
    this._addWrite(id, replacement);
  }

  delete<Table extends TableName>(
    tableName: Table | undefined,
    id: GenericId<Table>,
  ) {
    this._validateId(tableName, id);

    const document = this.get(tableName, id);
    if (document === null) {
      throw new Error("Delete on non-existent doc");
    }
    this._addWrite(id, null);
  }

  private _validateId(
    expectedTableName: unknown,
    id: unknown,
  ): asserts id is DocumentId {
    if (typeof id !== "string") {
      throw new Error(
        `Invalid argument \`id\`, expected string but got '${typeof id}': ${id as any}`,
      );
    }

    if (expectedTableName === undefined) {
      return;
    }

    if (typeof expectedTableName !== "string") {
      throw new Error(
        `Invalid argument \`tableName\`, expected string but got '${typeof expectedTableName}': ${expectedTableName as any}`,
      );
    }

    const actualTableName = tableNameFromId(id);
    if (actualTableName === null) {
      throw new Error(
        `Invalid argument \`id\`, expected ID value but got '${id}'`,
      );
    }

    if (actualTableName !== expectedTableName) {
      throw new Error(
        `Invalid argument \`id\`, expected ID in table '${expectedTableName}' but got ID in table '${actualTableName}'`,
      );
    }
  }

  commit(commitTs: bigint | null = null) {
    const lastWrites = this._writes.pop();
    if (!lastWrites) {
      throw new Error("Transaction already committed or rolled back");
    }
    for (const [id, write] of Object.entries(lastWrites)) {
      const _id = id as DocumentId;
      if (this._writes.length === 0) {
        const tableName = tableNameFromId(_id)!;
        if (write === null) {
          delete this._documents[_id];
          delete this._storage[_id];
          this._tableIds.get(tableName)?.delete(_id);
        } else {
          this._documents[_id] =
            commitTs === null ? write : resolveCommittedValues(write, commitTs);
          let tableIds = this._tableIds.get(tableName);
          if (tableIds === undefined) {
            tableIds = new Set();
            this._tableIds.set(tableName, tableIds);
          }
          tableIds.add(_id);
        }
      } else {
        this._addWrite(_id, write);
      }
    }
    const lastFiles = this._pendingFiles.pop()!;
    for (const [id, file] of lastFiles) {
      // Drop files whose `_storage` document was deleted before commit.
      if (this.get("_storage", id as GenericId<"_storage">) === null) {
        continue;
      }
      if (this._pendingFiles.length === 0) {
        this._storage[id] = file;
      } else {
        this.storeFile(id as GenericId<"_storage">, file);
      }
    }
  }

  rollbackWrites() {
    if (this._writes.length === 0) {
      throw new Error("Transaction already committed or rolled back");
    }
    this._writes.pop();
    this._pendingFiles.pop();
  }

  // Temporarily hide all pending writes so reads only see committed state.
  stashWrites(): Array<Record<DocumentId, StoredDocument | null>> {
    const stashed = this._writes;
    this._writes = [];
    return stashed;
  }

  restoreWrites(stashed: Array<Record<DocumentId, StoredDocument | null>>) {
    this._writes = stashed;
  }

  _validate(tableName: string, doc: GenericDocument) {
    if (this._schema === null || !this._schema.schemaValidation) {
      return;
    }
    const validator = this._schema.tables.get(tableName)?.documentType;
    if (validator === undefined) {
      return;
    }
    // Validate the pre-commit view, like the backend does, so an unresolved
    // commit timestamp is checked as the int64 it will become.
    validateValidator(validator, resolvePendingValues(doc));
  }

  startQuery(query: SerializedQuery) {
    const id = this._nextQueryId;
    const results = this._evaluateQuery(query);
    this._queryResults[id] = results;
    this._nextQueryId += 1;
    return id;
  }

  queryNext(queryId: QueryId) {
    const results = this._queryResults[queryId];
    if (results === undefined) {
      throw new Error("Bad queryId");
    }
    const { value, done } = results.next();
    if (done) {
      return { value: null, done: true };
    } else {
      return { value, done: false };
    }
  }

  private _encodeCursor(doc: GenericDocument, fieldPaths: string[]): string {
    const keys = fieldPaths.map((fp) => evaluateFieldPath(fp, doc));
    // null sentinel in the array = undefined field value.
    // Everything else is JSON.stringify(convexToJson(k)) producing a string,
    // so null vs string is unambiguous on decode.
    return JSON.stringify(
      keys.map((k) =>
        k === undefined ? null : JSON.stringify(convexToJson(k)),
      ),
    );
  }

  private _docIsAfterCursor(
    doc: GenericDocument,
    cursorKeys: any[],
    fieldPaths: string[],
    order: "asc" | "desc",
  ): number {
    const orderMultiplier = order === "asc" ? 1 : -1;
    for (let i = 0; i < fieldPaths.length; i++) {
      const docVal = evaluateFieldPath(fieldPaths[i], doc);
      const cursorVal = cursorKeys[i];
      const cmp = compareValues(docVal, cursorVal);
      if (cmp !== 0) return cmp * orderMultiplier;
    }
    return 0;
  }

  paginate({
    query,
    cursor,
    endCursor,
    pageSize,
    maximumRowsRead,
    maximumBytesRead,
    onRead,
  }: {
    query: SerializedQuery;
    cursor: string | null;
    endCursor?: string | null;
    pageSize: number;
    maximumRowsRead?: number | null;
    maximumBytesRead?: number | null;
    onRead: (doc: GenericDocument) => void;
  }) {
    const { sortedDocs, filterFn, fieldPathsToSortBy, order } =
      this._resolveQuerySource(query);

    // Always include _id in sort key fields for uniqueness
    const cursorFields = fieldPathsToSortBy.includes("_id")
      ? fieldPathsToSortBy
      : [...fieldPathsToSortBy, "_id"];

    const parseCursor = (c: string | null | undefined) => {
      if (!c) return null;
      // null = undefined field; strings are JSON.stringify(convexToJson(v))
      return (JSON.parse(c) as any[]).map((k) =>
        k === null ? undefined : jsonToConvex(JSON.parse(k)),
      );
    };

    const cursorKeys = cursor !== "_end_cursor" ? parseCursor(cursor) : null;
    // _end_cursor as endCursor means "to the end of the range" — no boundary
    const hasEndBoundary = !!endCursor && endCursor !== "_end_cursor";
    const endCursorKeys = hasEndBoundary ? parseCursor(endCursor) : null;

    const page: GenericDocument[] = [];
    // null cursor = start of range
    let isInPage = cursor === null;
    let isDone = false;
    let continueCursor: string = "_end_cursor";
    let splitCursor: string | null = null;
    let pageStatus: "SplitRecommended" | "SplitRequired" | null = null;
    let rowsRead = 0;
    let bytesRead = 0;
    let stoppedAtLimit = false;
    const readDocCursors: string[] = [];

    for (let i = 0; i < sortedDocs.length; i++) {
      const doc = sortedDocs[i];
      const isLastDoc = i === sortedDocs.length - 1;

      if (!isInPage) {
        if (cursor === "_end_cursor") {
          // Past the end — no doc is after the end cursor
          continue;
        } else if (cursorKeys) {
          // Sort-key-based cursor: skip docs at or before cursor position
          if (
            this._docIsAfterCursor(doc, cursorKeys, cursorFields, order) > 0
          ) {
            isInPage = true;
          } else {
            continue;
          }
        }
      }

      checkSearchCandidateLimit(query.source, rowsRead);

      // endCursor: stop when we reach or pass this position (inclusive boundary)
      if (hasEndBoundary && endCursorKeys) {
        const cmp = this._docIsAfterCursor(
          doc,
          endCursorKeys,
          cursorFields,
          order,
        );
        if (cmp >= 0) {
          // At or past end cursor — include this doc if exactly at cursor, then stop
          if (cmp === 0) {
            onRead(doc);
            rowsRead += 1;
            readDocCursors.push(this._encodeCursor(doc, cursorFields));
            if (filterFn(doc)) {
              page.push(doc);
            }
            continueCursor = this._encodeCursor(doc, cursorFields);
          }
          break;
        }
      }

      onRead(doc);
      rowsRead += 1;
      bytesRead += getDocumentSize(doc);
      readDocCursors.push(this._encodeCursor(doc, cursorFields));

      // Like the backend, read the complete interval when an end cursor is
      // supplied. Transaction limits still apply through onRead above.
      let hitLimit = false;
      if (!endCursor && maximumRowsRead && rowsRead >= maximumRowsRead) {
        hitLimit = true;
      }
      if (!endCursor && maximumBytesRead && bytesRead >= maximumBytesRead) {
        hitLimit = true;
      }

      if (filterFn(doc)) {
        page.push(doc);
      }

      if (hitLimit) {
        pageStatus = "SplitRequired";
        continueCursor = this._encodeCursor(doc, cursorFields);
        stoppedAtLimit = true;
        break;
      }

      if (!endCursor && page.length >= pageSize) {
        stoppedAtLimit = true;
        if (isLastDoc) {
          // No more docs — don't set continueCursor so isDone becomes true
        } else {
          continueCursor = this._encodeCursor(doc, cursorFields);
        }
        break;
      }
    }

    // Exhausting the source (including an endCursor-bounded range) requires
    // another read. Like streaming queries, check the cap before exhaustion.
    if (!stoppedAtLimit) {
      checkSearchCandidateLimit(query.source, rowsRead);
    }

    if (continueCursor === "_end_cursor") {
      isDone = true;
    }

    // Compute splitCursor at midpoint when limits are hit
    if (pageStatus === "SplitRequired" && readDocCursors.length >= 2) {
      const midIdx = Math.floor((readDocCursors.length - 1) / 2);
      splitCursor = readDocCursors[midIdx];
    } else if (
      pageStatus === null &&
      rowsRead > pageSize + 1 &&
      readDocCursors.length >= 2
    ) {
      // Recommend split when we had to scan significantly more rows than pageSize
      pageStatus = "SplitRecommended";
      const midIdx = Math.floor((readDocCursors.length - 1) / 2);
      splitCursor = readDocCursors[midIdx];
    }

    return {
      page,
      isDone,
      continueCursor,
      splitCursor,
      pageStatus,
    };
  }

  private _iterateDocs(
    tableName: string,
    callback: (doc: GenericDocument) => void,
  ) {
    const isInTable = (id: string) => tableNameFromId(id) === tableName;
    const ids = new Set<string>(this._tableIds.get(tableName));
    for (let i = 0; i < this._writes.length; i++) {
      for (const id of Object.keys(this._writes[i]).filter(isInTable)) {
        ids.add(id);
      }
    }
    for (const id of ids) {
      const document = this.get(tableName, id as DocumentId);
      if (document !== null) {
        callback(document);
      }
    }
  }

  /**
   * Resolves the query source: loads documents matching the source (table scan,
   * index range, or search), sorts them, and returns along with the compiled
   * operator filter function. This separates "rows entering the pipeline"
   * (sortedDocs) from "rows exiting" (after filterFn), which is needed for
   * maximumRowsRead tracking in paginate().
   */
  private _resolveQuerySource(query: SerializedQuery): {
    sortedDocs: GenericDocument[];
    filterFn: (doc: GenericDocument) => boolean;
    limit: number | null;
    fieldPathsToSortBy: string[];
    order: "asc" | "desc";
  } {
    const source = query.source;
    const results: GenericDocument[] = [];
    let fieldPathsToSortBy: string[];
    let order: "asc" | "desc";
    switch (source.type) {
      case "FullTableScan": {
        const tableName = source.tableName;
        this._iterateDocs(tableName, (doc) => {
          results.push(doc);
        });
        order = source.order ?? "asc";
        fieldPathsToSortBy = ["_creationTime"];

        break;
      }
      case "IndexRange": {
        const [tableName, indexName] = source.indexName.split(".");
        order = source.order ?? "asc";
        let fields;
        if (indexName === "by_creation_time") {
          fields = ["_creationTime", "_id"];
        } else if (indexName === "by_id") {
          fields = ["_id"];
        } else {
          const indexes = this._schema?.tables.get(tableName)?.indexes;
          const index = indexes?.find(
            ({ indexDescriptor }: { indexDescriptor: string }) =>
              indexDescriptor === indexName,
          );
          if (index === undefined) {
            throw new Error(
              `Cannot use index "${indexName}" for table "${tableName}" because ` +
                `it is not declared in the schema.`,
            );
          }
          fields = index.fields.concat(["_creationTime", "_id"]);
        }
        fieldPathsToSortBy = fields;

        validateIndexRangeExpression(source, fields);

        this._iterateDocs(tableName, (doc) => {
          if (
            source.range.every((filter) => evaluateRangeFilter(doc, filter))
          ) {
            results.push(doc);
          }
        });
        break;
      }
      case "Search": {
        const [tableName] = source.indexName.split(".");
        this._iterateDocs(tableName, (doc) => {
          if (
            source.filters.every((filter) => evaluateSearchFilter(doc, filter))
          ) {
            results.push(doc);
          }
        });
        fieldPathsToSortBy = [];
        order = "asc";
        break;
      }
    }

    const filters = query.operators
      .filter(
        (operator): operator is { filter: FilterJson } => "filter" in operator,
      )
      .map((operator) => operator.filter);

    const limit =
      query.operators.filter(
        (operator): operator is { limit: number } => "limit" in operator,
      )[0] ?? null;

    results.sort((a, b) => {
      const orderMultiplier = order === "asc" ? 1 : -1;
      let v = 0;
      for (const fp of fieldPathsToSortBy) {
        v = compareValues(evaluateFieldPath(fp, a), evaluateFieldPath(fp, b));
        if (v !== 0) {
          return v * orderMultiplier;
        }
      }
      return v * orderMultiplier;
    });

    const filterFn = (doc: GenericDocument) =>
      filters.every((f) => evaluateFilter(doc, f));

    return {
      sortedDocs: results,
      filterFn,
      limit: limit?.limit ?? null,
      fieldPathsToSortBy,
      order,
    };
  }

  private _evaluateQuery(query: SerializedQuery): Iterator<GenericDocument> {
    const { sortedDocs, filterFn, limit } = this._resolveQuerySource(query);
    // Snapshot the source above, but consume candidates lazily so take() and
    // early exits from iteration don't scan past the search limit.
    return (function* () {
      let scanned = 0;
      let returned = 0;
      while (limit === null || returned < limit) {
        // Like the backend, check before testing for exhaustion and before
        // applying post-search filters. Search index equality filters have
        // already been applied by _resolveQuerySource().
        checkSearchCandidateLimit(query.source, scanned);
        if (scanned === sortedDocs.length) return;
        const doc = sortedDocs[scanned++];
        if (filterFn(doc)) {
          returned++;
          yield doc;
        }
      }
    })();
  }

  vectorSearch(
    tableAndIndexName: string,
    vector: number[],
    expressions: SerializedRangeExpression[],
    limit: number,
  ) {
    const results: GenericDocument[] = [];
    const [tableName, indexName] = tableAndIndexName.split(".");
    this._iterateDocs(tableName, (doc) => {
      if (expressions.every((filter) => evaluateFilter(doc, filter))) {
        results.push(doc);
      }
    });
    const vectorIndexes = this._schema?.tables.get(tableName)?.vectorIndexes;
    const vectorIndex = vectorIndexes?.find(
      ({ indexDescriptor }: { indexDescriptor: string }) =>
        indexDescriptor === indexName,
    );
    if (vectorIndex === undefined) {
      throw new Error(
        `Cannot use vector index "${indexName}" for table "${tableName}" because ` +
          `it is not declared in the schema.`,
      );
    }
    const { vectorField } = vectorIndex;
    const idsAndScores = results.map((doc) => {
      const score = cosineSimilarity(vector, doc[vectorField] as number[]);
      return { _id: doc._id, _score: score };
    });
    idsAndScores.sort((a, b) => b._score - a._score);
    return idsAndScores.slice(0, limit);
  }

  validateSchema() {
    this._schema?.tables.forEach((table, tableName) => {
      if (!isValidIdentifier(tableName)) {
        throw new Error(
          `Table names must be valid identifiers, got "${tableName}"`,
        );
      }

      validateFieldNames(table.documentType);

      table.indexes.forEach(({ indexDescriptor }) => {
        if (!isValidIdentifier(indexDescriptor)) {
          throw new Error(
            `Index names must be valid identifiers, got "${indexDescriptor}"`,
          );
        }
      });
    });
  }

  get componentPath() {
    return this._componentPath;
  }
}

function tableNameFromId(id: string) {
  const match = id.match(/^[0-9]+/);
  if (!match || match[0].length === id.length) {
    return null;
  }
  return id.slice(match[0].length);
}

// Reverses `serializeConvexErrorData` from `convex/server/impl/registration_impl`,
// which `invokeFunction` applies to every ConvexError thrown from a Convex
// function. In production the HTTP client deserializes `.data` via
// `forwardErrorData` before callers see it, so an action catching a
// `ConvexError` from `ctx.runMutation` receives the original object, not a
// JSON string. Apply the same restoration whenever we catch an error from
// `invokeMutation` / `invokeQuery` / `invokeAction`.
function deserializeConvexErrorData(thrown: unknown) {
  if (
    typeof thrown === "object" &&
    thrown !== null &&
    Symbol.for("ConvexError") in thrown &&
    typeof (thrown as { data: unknown }).data === "string"
  ) {
    try {
      (thrown as { data: unknown }).data = jsonToConvex(
        JSON.parse((thrown as { data: string }).data),
      );
    } catch {
      // Data wasn't a serialized JSON string — leave it untouched.
    }
  }
  return thrown;
}

function isSimpleObject(value: unknown) {
  const isObject = typeof value === "object";
  const prototype = Object.getPrototypeOf(value);
  const isSimple =
    prototype === null ||
    prototype === Object.prototype ||
    // Objects generated from other contexts (e.g. across Node.js `vm` modules) will not satisfy the previous
    // conditions but are still simple objects.
    prototype?.constructor?.name === "Object";
  return isObject && isSimple;
}

// Commit timestamp placeholders are resolved to `i64::MAX` before commit.
const MAX_COMMIT_TS = 2n ** 63n - 1n;

function hasPendingValues(value: Value): boolean {
  if (value instanceof CommitTsPlaceholder) {
    return true;
  }
  if (Array.isArray(value)) {
    return value.some(hasPendingValues);
  }
  if (value !== null && value !== undefined && isSimpleObject(value)) {
    return Object.values(value).some(hasPendingValues);
  }
  return false;
}

// The value as the transaction sees it before committing.
function resolvePendingValues(value: Value): Value {
  return substituteCommitTs(value, MAX_COMMIT_TS);
}

// The value as it is committed.
function resolveCommittedValues<T extends Value>(
  value: T,
  commitTs: bigint,
): T {
  return substituteCommitTs(value, commitTs);
}

function substituteCommitTs<T extends Value>(value: T, commitTs: bigint): T {
  if (!hasPendingValues(value)) {
    return value;
  }
  if (value instanceof CommitTsPlaceholder) {
    return commitTs as T;
  }
  if (Array.isArray(value)) {
    return value.map((v) => substituteCommitTs(v, commitTs)) as T;
  }
  return Object.fromEntries(
    Object.entries(value as object).map(([k, v]) => [
      k,
      substituteCommitTs(v, commitTs),
    ]),
  ) as T;
}

function evaluateFieldPath(fieldPath: string, document: any) {
  const pathParts = fieldPath.split(".");
  let result: Value | undefined = document;
  for (const p of pathParts) {
    result =
      result !== undefined && result !== null && isSimpleObject(result)
        ? (result as any)[p]
        : undefined;
  }
  return result === undefined ? undefined : resolvePendingValues(result);
}

function evaluateFilter(
  document: GenericDocument,
  filter: any,
): Value | undefined {
  if (filter.$eq !== undefined) {
    return (
      compareValues(
        evaluateFilter(document, filter.$eq[0]),
        evaluateFilter(document, filter.$eq[1]),
      ) === 0
    );
  }
  if (filter.$neq !== undefined) {
    return (
      compareValues(
        evaluateFilter(document, filter.$neq[0]),
        evaluateFilter(document, filter.$neq[1]),
      ) !== 0
    );
  }
  if (filter.$and !== undefined) {
    return filter.$and.every((child: any) => evaluateFilter(document, child));
  }
  if (filter.$or !== undefined) {
    return filter.$or.some((child: any) => evaluateFilter(document, child));
  }
  if (filter.$not !== undefined) {
    return !evaluateFilter(document, filter.$not);
  }
  if (filter.$gt !== undefined) {
    return (
      evaluateFilter(document, filter.$gt[0])! >
      evaluateFilter(document, filter.$gt[1])!
    );
  }
  if (filter.$gte !== undefined) {
    return (
      evaluateFilter(document, filter.$gte[0])! >=
      evaluateFilter(document, filter.$gte[1])!
    );
  }
  if (filter.$lt !== undefined) {
    return (
      evaluateFilter(document, filter.$lt[0])! <
      evaluateFilter(document, filter.$lt[1])!
    );
  }
  if (filter.$lte !== undefined) {
    return (
      evaluateFilter(document, filter.$lte[0])! <=
      evaluateFilter(document, filter.$lte[1])!
    );
  }
  if (filter.$add !== undefined) {
    return (
      (evaluateFilter(document, filter.$add[0]) as number) +
      (evaluateFilter(document, filter.$add[1]) as number)
    );
  }
  if (filter.$sub !== undefined) {
    return (
      (evaluateFilter(document, filter.$sub[0]) as number) -
      (evaluateFilter(document, filter.$sub[1]) as number)
    );
  }
  if (filter.$mul !== undefined) {
    return (
      (evaluateFilter(document, filter.$mul[0]) as number) *
      (evaluateFilter(document, filter.$mul[1]) as number)
    );
  }
  if (filter.$div !== undefined) {
    return (
      (evaluateFilter(document, filter.$div[0]) as number) /
      (evaluateFilter(document, filter.$div[1]) as number)
    );
  }
  if (filter.$mod !== undefined) {
    return (
      (evaluateFilter(document, filter.$mod[0]) as number) %
      (evaluateFilter(document, filter.$mod[1]) as number)
    );
  }
  if (filter.$field !== undefined) {
    return evaluateFieldPath(filter.$field, document);
  }
  if (filter.$literal !== undefined) {
    return evaluateQueryValue(filter.$literal);
  }
  throw new Error(`not implemented: ${JSON.stringify(filter)}`);
}

function evaluateRangeFilter(
  document: GenericDocument,
  expr: SerializedRangeExpression,
) {
  const result = evaluateFieldPath(expr.fieldPath, document);
  const value = evaluateQueryValue(expr.value);
  switch (expr.type) {
    case "Eq":
      return compareValues(result, value) === 0;
    case "Gt":
      return compareValues(result, value) > 0;
    case "Gte":
      return compareValues(result, value) >= 0;
    case "Lt":
      return compareValues(result, value) < 0;
    case "Lte":
      return compareValues(result, value) <= 0;
  }
}

// A value used as a written field.
function evaluateValue(value: JSONValue) {
  if (typeof value === "object" && value !== null && "$undefined" in value) {
    return undefined;
  }
  return jsonToConvex(value);
}

// A value used as a query bound or literal.
function evaluateQueryValue(value: JSONValue) {
  const evaluatedValue = evaluateValue(value);
  return evaluatedValue === undefined
    ? undefined
    : resolvePendingValues(evaluatedValue);
}

function checkSearchCandidateLimit(source: Source, scanned: number) {
  if (source.type === "Search" && scanned >= MAX_SEARCH_CANDIDATE_REVISIONS) {
    throw new Error(
      `Search query scanned too many documents (fetched ${MAX_SEARCH_CANDIDATE_REVISIONS}). ` +
        "Consider using a smaller limit, paginating the query, or using a filter " +
        "field to limit the number of documents pulled from the search index.",
    );
  }
}

function evaluateSearchFilter(
  document: GenericDocument,
  filter: SerializedSearchFilter,
) {
  const result = evaluateFieldPath(filter.fieldPath, document);
  switch (filter.type) {
    case "Eq":
      return compareValues(result, evaluateQueryValue(filter.value)) === 0;
    case "Search": {
      if (typeof result !== "string") {
        return false;
      }
      const queryTerms = filter.value
        .toLowerCase()
        .split(/\s+/)
        .filter((term) => term.length > 0);
      const documentWords = result
        .split(/\s+/)
        .map((word) => word.toLowerCase());
      return queryTerms.some((queryTerm) =>
        documentWords.some((word) => word.startsWith(queryTerm)),
      );
    }
  }
}

function cosineSimilarity(vecA: number[], vecB: number[]): number {
  let dotProduct = 0.0;
  let normA = 0.0;
  let normB = 0.0;

  for (let i = 0; i < vecA.length; i++) {
    dotProduct += vecA[i] * vecB[i];
    normA += vecA[i] * vecA[i];
    normB += vecB[i] * vecB[i];
  }

  if (normA === 0 || normB === 0) {
    return 0;
  } else {
    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
  }
}

// An index range expression is always a chained list of:
//
// 0 or more equality expressions defined with .eq.
// [Optionally] A lower bound expression defined with .gt or .gte.
// [Optionally] An upper bound expression defined with .lt or .lte.
function validateIndexRangeExpression(
  source: Source & { type: "IndexRange" },
  fields: string[],
) {
  let fieldIndex = 0;
  let state: "eq" | "gt" | "lt" | "done" = "eq";
  for (const [filterIndex, filter] of source.range.entries()) {
    if (state === "done") {
      throw new Error(
        `Incorrect operator used in \`withIndex\`, cannot chain ` +
          `more operators after both \`.gt\` and \`.lt\` were already used, ` +
          `got \`${printIndexOperator(filter)}\`.`,
      );
    }

    const filterType: "eq" | "gt" | "lt" =
      filter.type == "Gt" || filter.type == "Gte"
        ? "gt"
        : filter.type == "Lt" || filter.type == "Lte"
          ? "lt"
          : "eq";

    switch (`${state}|${filterType}`) {
      // Allow to operate on the current indexed field
      case "eq|eq":
      case "eq|gt":
      case "eq|lt":
        if (filter.fieldPath === fields[fieldIndex]) {
          fieldIndex += 1;
          state = filterType;
          continue;
        }
        throw new Error(
          `Incorrect field used in \`withIndex\`, ` +
            `expected "${fields[fieldIndex]}", got "${filter.fieldPath}"`,
        );

      // Allow to operate on the previous field (gt and lt must operate on same field)
      case "lt|gt":
      case "gt|lt":
        if (fieldIndex > 0 && filter.fieldPath === fields[fieldIndex - 1]) {
          state = "done";
          continue;
        }
        throw new Error(
          `Incorrect field used in \`withIndex\`, ` +
            `\`.gt\` and \`.lt\` must operate on the same field, ` +
            `expected "${fields[fieldIndex - 1]}", got "${filter.fieldPath}"`,
        );

      default:
        throw new Error(
          `Incorrect operator used in \`withIndex\`, ` +
            `cannot chain \`.${filter.type.toLowerCase()}()\` ` +
            `after \`.${source.range[filterIndex - 1].type.toLowerCase()}()\``,
        );
    }
  }
}

function printIndexOperator(filter: SerializedRangeExpression) {
  return `.${filter.type.toLowerCase()}(${filter.fieldPath}, ${JSON.stringify(filter.value)})`;
}

function validateFieldNames(validator: ValidatorJSON) {
  if (validator.type === "object") {
    Object.keys(validator.value).forEach((fieldName) => {
      if (!isValidIdentifier(fieldName)) {
        throw new Error(
          `Field names must be valid identifiers, got "${fieldName}"`,
        );
      }
    });
  }

  if (validator.type === "union") {
    validator.value.forEach(validateFieldNames);
  }
}

function isValidIdentifier(name: string) {
  return /(^_(id|creationTime)$)|^[a-zA-Z][a-zA-Z0-9_]*$/.test(name);
}

type ObjectFieldType = { fieldType: ValidatorJSON; optional: boolean };

type ValidatorJSON =
  | {
      type: "null";
    }
  | { type: "number" }
  | { type: "bigint" }
  | { type: "commitTs" }
  | { type: "boolean" }
  | { type: "string" }
  | { type: "bytes" }
  | { type: "function" }
  | { type: "any" }
  | {
      type: "literal";
      value: JSONValue;
    }
  | { type: "id"; tableName: string }
  | { type: "array"; value: ValidatorJSON }
  | { type: "object"; value: Record<string, ObjectFieldType> }
  | { type: "union"; value: ValidatorJSON[] };

function validateValidator(validator: ValidatorJSON, value: any) {
  switch (validator.type) {
    case "null": {
      if (value !== null) {
        throw new Error(`Validator error: Expected \`null\`, got \`${value}\``);
      }
      return;
    }
    case "number": {
      if (typeof value !== "number") {
        throw new Error(
          `Validator error: Expected \`number\`, got \`${value}\``,
        );
      }
      return;
    }
    case "bigint": {
      if (typeof value !== "bigint") {
        throw new Error(
          `Validator error: Expected \`bigint\`, got \`${value}\``,
        );
      }
      return;
    }
    case "commitTs": {
      if (
        typeof value !== "bigint" &&
        !(value instanceof CommitTsPlaceholder)
      ) {
        throw new Error(
          `Validator error: Expected \`v.commitTs()\`, got \`${value}\``,
        );
      }
      return;
    }
    case "boolean": {
      if (typeof value !== "boolean") {
        throw new Error(
          `Validator error: Expected \`boolean\`, got \`${value}\``,
        );
      }
      return;
    }
    case "string": {
      if (typeof value !== "string") {
        throw new Error(
          `Validator error: Expected \`string\`, got \`${value}\``,
        );
      }
      return;
    }
    case "bytes": {
      if (!(value instanceof ArrayBuffer)) {
        throw new Error(
          `Validator error: Expected \`ArrayBuffer\`, got \`${value}\``,
        );
      }
      return;
    }
    case "function": {
      if (typeof value !== "string" || !value.startsWith("function://")) {
        throw new Error(
          `Validator error: Expected \`v.function()\`, got \`${value}\``,
        );
      }
      return;
    }
    case "any": {
      return;
    }
    case "literal": {
      if (value !== validator.value) {
        throw new Error(
          `Validator error: Expected \`${
            validator.value as any
          }\`, got \`${value}\``,
        );
      }
      return;
    }
    case "id": {
      if (typeof value !== "string") {
        throw new Error(
          `Validator error: Expected \`string\`, got \`${value}\``,
        );
      }
      if (tableNameFromId(value) !== validator.tableName) {
        throw new Error(
          `Validator error: Expected ID for table "${validator.tableName}", got \`${value}\``,
        );
      }
      return;
    }
    case "array": {
      if (!Array.isArray(value)) {
        throw new Error(
          `Validator error: Expected \`Array\`, got \`${value}\``,
        );
      }
      for (const v of value) {
        validateValidator(validator.value, v);
      }
      return;
    }
    case "union": {
      let isValid = false;
      for (const v of validator.value) {
        try {
          validateValidator(v, value);
          isValid = true;
          break;
        } catch {
          // Ignore
        }
      }
      if (!isValid) {
        throw new Error(
          `Validator error: Expected one of ${validator.value.map((v) => v.type).join(", ")}, got \`${JSON.stringify(convexToJson(value))}\``,
        );
      }
      return;
    }
    case "object": {
      if (typeof value !== "object") {
        throw new Error(
          `Validator error: Expected \`object\`, got \`${value}\``,
        );
      }
      if (!isSimpleObject(value)) {
        throw new Error(
          `Validator error: Expected a plain old JavaScript \`object\`, got \`${value}\``,
        );
      }
      for (const [k, { fieldType, optional }] of Object.entries(
        validator.value,
      )) {
        if (value[k] === undefined) {
          if (!optional) {
            throw new Error(
              `Validator error: Missing required field \`${k}\` in object`,
            );
          }
        } else {
          validateValidator(fieldType, value[k]);
        }
      }
      for (const k of Object.keys(value)) {
        if (validator.value[k] === undefined) {
          throw new Error(
            `Validator error: Unexpected field \`${k}\` in object`,
          );
        }
      }
      return;
    }
  }
}

function syscallImpl() {
  return (op: string, jsonArgs: string) => {
    const args = JSON.parse(jsonArgs);
    const db = getDb();
    switch (op) {
      case "1.0/getSnapshotTs": {
        return JSON.stringify(
          convexToJson(getTransactionManager().getSnapshotTs()),
        );
      }
      case "1.0/queryStream": {
        const { query } = args;
        const tracker = getTransactionManager().getMetricsTracker();
        tracker?.trackIndexRange();
        const queryId = db.startQuery(query);
        return JSON.stringify({ queryId });
      }
      case "1.0/queryCleanup": {
        return JSON.stringify({});
      }
      case "1.0/db/normalizeId": {
        const idString: string = args.idString;
        const tableName = tableNameFromId(idString);
        return JSON.stringify({
          id: tableName === args.table ? idString : null,
        });
      }
      default: {
        throw new Error(`\`convexTest\` does not support syscall: "${op}"`);
      }
    }
  };
}

class AuthFake {
  constructor(
    private _userIdentity: any = null,
    // The JWT the request was authenticated with, mirroring what the backend
    // derives the identity from. `null` for unauthenticated requests.
    private _token: string | null = null,
  ) {}

  getUserIdentity(): Promise<any> {
    return Promise.resolve(this._userIdentity);
  }

  get token(): string | null {
    return this._token;
  }
}

// Maps the `UserIdentity` attributes to the JWT claims they are derived from,
// as documented on `UserIdentity`. `tokenIdentifier` is left out because it is
// computed by the backend from `iss` and `sub` rather than being a claim.
const JWT_CLAIM_BY_IDENTITY_ATTRIBUTE: Record<string, string> = {
  subject: "sub",
  issuer: "iss",
  name: "name",
  givenName: "given_name",
  familyName: "family_name",
  nickname: "nickname",
  preferredUsername: "preferred_username",
  profileUrl: "profile",
  pictureUrl: "picture",
  email: "email",
  emailVerified: "email_verified",
  gender: "gender",
  birthday: "birthdate",
  timezone: "zoneinfo",
  language: "locale",
  phoneNumber: "phone_number",
  phoneNumberVerified: "phone_number_verified",
  address: "address",
  updatedAt: "updated_at",
};

function base64UrlEncode(value: string) {
  return base64UrlEncodeBytes(new TextEncoder().encode(value));
}

function base64UrlEncodeBytes(bytes: Uint8Array) {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

// Builds the token `withIdentity` pretends the request was authenticated with.
// Real tokens are signed by an identity provider; this one is unsecured
// (`"alg": "none"`, empty signature).
function createTestJwt(identity: Record<string, any>) {
  const claims: Record<string, any> = Object.create(null);
  for (const [attribute, value] of Object.entries(identity)) {
    if (value === undefined || attribute === "tokenIdentifier") {
      continue;
    }
    // Custom claims are used as is, known attributes use their claim name.
    const claimName = Object.hasOwn(JWT_CLAIM_BY_IDENTITY_ATTRIBUTE, attribute)
      ? JWT_CLAIM_BY_IDENTITY_ATTRIBUTE[attribute]
      : attribute;
    claims[claimName] = value;
  }
  // The identity provider always determines who the user is.
  claims.iss = identity.issuer;
  claims.sub = identity.subject;
  const header = base64UrlEncode(JSON.stringify({ alg: "none", typ: "JWT" }));
  const payload = base64UrlEncode(JSON.stringify(claims));
  return `${header}.${payload}.`;
}

// Limits the backend puts on tokens from `ctx.auth.signJwt`.
const DEPLOYMENT_JWT_MAX_LIFETIME_SECONDS = 60 * 60;
const DEPLOYMENT_JWT_MAX_SIZE = 8000;
const RESERVED_JWT_CLAIM_PREFIXES = ["convex", "cvx"];
// Used as the `iss` origin when the test environment doesn't set
// `CONVEX_CLOUD_URL`, which the backend always does.
const DEFAULT_CONVEX_CLOUD_URL = "https://some-deployment.convex.cloud";

// `ctx.auth` in mutations, which can also sign JWTs with the deployment's key.
// Signing goes through the `1.0/signJwt` syscall, like `convex/server` does.
function mutationAuth(auth: AuthFake) {
  return {
    getUserIdentity: () => auth.getUserIdentity(),
    signJwt: async (options: any) =>
      JSON.parse(
        await getSyscalls().asyncSyscall(
          "1.0/signJwt",
          JSON.stringify(options ?? {}),
        ),
      ),
  };
}

// Mirrors the backend's `1.0/signJwt` syscall, including its validation and
// its order. Tokens are signed with a key generated for the `convexTest`
// instance rather than the deployment's key.
async function signDeploymentJwt(args: any): Promise<string> {
  const { claims: rawClaims = {}, expiresInSeconds } = args ?? {};
  if (
    typeof rawClaims !== "object" ||
    rawClaims === null ||
    Array.isArray(rawClaims)
  ) {
    throw new Error(
      "Invalid arguments for auth.signJwt: claims must be an object",
    );
  }
  if (
    typeof expiresInSeconds !== "number" ||
    !Number.isFinite(expiresInSeconds) ||
    expiresInSeconds < 1
  ) {
    throw new Error(
      "Invalid arguments for auth.signJwt: expiresInSeconds must be a number of seconds, at least 1",
    );
  }
  const claims: Record<string, any> = { ...rawClaims };
  for (const reserved of ["iss", "iat", "exp"]) {
    if (Object.hasOwn(claims, reserved)) {
      throw new Error(
        `The '${reserved}' claim is set by Convex and can't be provided`,
      );
    }
  }
  const reservedPrefixClaim = Object.keys(claims).find((name) =>
    RESERVED_JWT_CLAIM_PREFIXES.some(
      (prefix) =>
        name.slice(0, prefix.length).toLowerCase() === prefix.toLowerCase(),
    ),
  );
  if (reservedPrefixClaim !== undefined) {
    throw new Error(
      `The '${reservedPrefixClaim}' claim can't be provided: claim names ` +
        `starting with 'convex' or 'cvx' are reserved`,
    );
  }
  if (
    Object.hasOwn(claims, "sub") &&
    (typeof claims.sub !== "string" || claims.sub === "")
  ) {
    throw new Error("The 'sub' claim must be a non-empty string");
  }
  // Every token gets a `jti` so verifiers can detect replays. Callers may
  // pick their own.
  if (!Object.hasOwn(claims, "jti")) {
    claims.jti = base64UrlEncodeBytes(
      crypto.getRandomValues(new Uint8Array(16)),
    );
  } else if (typeof claims.jti !== "string" || claims.jti === "") {
    throw new Error("The 'jti' claim must be a non-empty string");
  }
  if (Object.hasOwn(claims, "nbf")) {
    throw new Error(
      "The 'nbf' claim is reserved by Convex and can't be provided",
    );
  }
  validateDeploymentJwtAudience(claims);
  // The backend truncates the lifetime to whole seconds.
  const expiresIn = Math.floor(expiresInSeconds);
  if (expiresIn > DEPLOYMENT_JWT_MAX_LIFETIME_SECONDS) {
    throw new Error(
      `expiresInSeconds must be between 1 second and ${DEPLOYMENT_JWT_MAX_LIFETIME_SECONDS} seconds`,
    );
  }

  const issuedAt = Math.floor(Date.now() / 1000);
  const payload = {
    ...claims,
    iss: deploymentJwtIssuer(getCurrentComponentPath()),
    iat: issuedAt,
    exp: issuedAt + expiresIn,
  };
  const { privateKey, kid } = await getDeploymentJwtKey();
  const header = { alg: "ES256", typ: "JWT", kid };
  const signingInput =
    base64UrlEncode(JSON.stringify(header)) +
    "." +
    base64UrlEncode(JSON.stringify(payload));
  const signature = await crypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" },
    privateKey,
    new TextEncoder().encode(signingInput),
  );
  const token = `${signingInput}.${base64UrlEncodeBytes(new Uint8Array(signature))}`;
  if (token.length > DEPLOYMENT_JWT_MAX_SIZE) {
    throw new Error(
      `The token is ${token.length} bytes, over the ${DEPLOYMENT_JWT_MAX_SIZE} byte limit`,
    );
  }
  return token;
}

// Every token is signed with the same key, so `aud` is the only claim a
// verifier can check to reject a token that was meant for another verifier.
function validateDeploymentJwtAudience(claims: Record<string, any>) {
  const isAudience = (aud: unknown) => typeof aud === "string" && aud !== "";
  if (!Object.hasOwn(claims, "aud")) {
    throw new Error(
      "The 'aud' claim is required. Set it to the audience that verifies the token.",
    );
  }
  const aud = claims.aud;
  const valid = Array.isArray(aud)
    ? aud.length > 0 && aud.every(isAudience)
    : isAudience(aud);
  if (!valid) {
    throw new Error(
      "The 'aud' claim must be a non-empty string or a non-empty array of non-empty strings",
    );
  }
}

// The app signs under the deployment's origin and a component under
// `{origin}/components/{path}`.
function deploymentJwtIssuer(componentPath: string) {
  // Signing is allowed in inits, which can't read environment variables.
  const cloudUrl = executionContextStorage.exit(
    () => typeof process !== "undefined" && process.env.CONVEX_CLOUD_URL,
  );
  const origin = (cloudUrl || DEFAULT_CONVEX_CLOUD_URL).replace(/\/+$/, "");
  return componentPath === ROOT_COMPONENT_PATH
    ? origin
    : `${origin}/components/${componentPath}`;
}

function getDeploymentJwtKey() {
  const convex = getConvexGlobal();
  convex.deploymentJwtKey ??= generateDeploymentJwtKey();
  return convex.deploymentJwtKey;
}

async function generateDeploymentJwtKey(): Promise<DeploymentJwtKey> {
  const { privateKey, publicKey } = await crypto.subtle.generateKey(
    { name: "ECDSA", namedCurve: "P-256" },
    false,
    ["sign", "verify"],
  );
  const { x, y } = await crypto.subtle.exportKey("jwk", publicKey);
  // Like the backend, use the key's RFC 7638 thumbprint as its `kid`.
  const thumbprint = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(
      JSON.stringify({ crv: "P-256", kty: "EC", x: x!, y: y! }),
    ),
  );
  const kid = base64UrlEncodeBytes(new Uint8Array(thumbprint));
  return {
    privateKey,
    kid,
    publicJwk: {
      kty: "EC",
      crv: "P-256",
      x: x!,
      y: y!,
      kid,
      alg: "ES256",
      use: "sig",
    },
  };
}

// What the backend serves at `${CONVEX_CLOUD_URL}/.well-known/jwks.json`.
async function deploymentJwks(): Promise<DeploymentJwks> {
  const { publicJwk } = await getDeploymentJwtKey();
  return { keys: [{ ...publicJwk }] };
}

// Runs a function that was scheduled, moving its `_scheduled_functions`
// document through the job states around calling it. Scheduled functions run
// without auth context, even if they were scheduled from an authenticated
// function.
async function runScheduledFunction({
  db,
  jobId,
  componentPath,
  functionPath,
  args,
  name,
}: {
  db: DatabaseFake;
  jobId: GenericId<"_scheduled_functions">;
  componentPath: string;
  functionPath: FunctionPath;
  args: Value;
  name: string;
}) {
  // A scheduled function is its own request: it doesn't inherit the request
  // metadata of whoever scheduled it, and it identifies itself through its
  // job ID.
  const requestMetadata: RequestMetadataFake = {
    ip: null,
    userAgent: null,
    requestId: newRequestId(),
    scheduledFunctionId: jobId,
    authToken: null,
  };
  await requestMetadataStorage.run(requestMetadata, () =>
    authStorage.run(new AuthFake(), async () => {
      const canceled = await withAuth().runInComponent(
        componentPath,
        async () => {
          const job = db.get("_scheduled_functions", jobId) as
            ScheduledFunction | undefined;
          if (!job) return true;
          if (job.state.kind === "canceled") {
            return true;
          }
          if (job.state.kind !== "pending") {
            throw new Error(
              `\`convexTest\` invariant error: Unexpected scheduled function state when starting it: ${job.state.kind}`,
            );
          }
          db.patch("_scheduled_functions", jobId, {
            state: { kind: "inProgress" },
          });
          return false;
        },
      );
      if (canceled) {
        return;
      }
      try {
        await withAuth().fun(functionPath, args);
      } catch (error) {
        console.error(`Error when running scheduled function ${name}`, error);
        await withAuth().runInComponent(componentPath, async () => {
          db.patch("_scheduled_functions", jobId, {
            state: { kind: "failed" },
            completedTime: Date.now(),
          });
        });
        return;
      }
      await withAuth().runInComponent(componentPath, async () => {
        const job = db.get("_scheduled_functions", jobId) as ScheduledFunction;
        if (job.state.kind === "canceled") {
          return;
        }
        if (job.state.kind !== "inProgress") {
          throw new Error(
            `\`convexTest\` invariant error: Unexpected scheduled function state after it finished running: ${job.state.kind}`,
          );
        }
        db.patch("_scheduled_functions", jobId, {
          state: { kind: "success" },
        });
      });
    }),
  );
}

function asyncSyscallImpl() {
  return async (op: string, jsonArgs: string): Promise<string> => {
    const args = JSON.parse(jsonArgs);
    const db = getDb();
    const tracker = getTransactionManager().getMetricsTracker();
    function getAndTrack(table: string, id: GenericId<string>) {
      const doc = db.get(table, id);
      if (doc !== null) {
        tracker?.trackRead(getDocumentSize(doc));
      }
      return doc;
    }
    switch (op) {
      case "1.0/get": {
        const { table, id } = args;
        tracker?.trackIndexRange();
        const doc = getAndTrack(table, id);
        return JSON.stringify(convexToJson(doc));
      }
      case "1.0/queryStreamNext": {
        const { value, done } = db.queryNext(args.queryId);
        if (!done && value !== null) {
          tracker?.trackRead(getDocumentSize(value));
        }
        return JSON.stringify(convexToJson({ value, done }));
      }
      case "1.0/queryPage": {
        const {
          query,
          cursor,
          endCursor,
          pageSize,
          maximumRowsRead,
          maximumBytesRead,
        } = args;
        const pagination = executionContextStorage.getStore()?.pagination;
        if (pagination) {
          pagination.calls += 1;
          if (pagination.calls > 1) {
            throw new Error(
              "Only a single paginated query (`.paginate()`) is allowed per " +
                "query, including nested query calls and component calls. " +
                "This is a Convex limit: https://docs.convex.dev/database/pagination",
            );
          }
        }
        tracker?.trackIndexRange();
        const { page, isDone, continueCursor, splitCursor, pageStatus } =
          db.paginate({
            query,
            cursor,
            endCursor,
            pageSize,
            maximumRowsRead,
            maximumBytesRead,
            // Charge every row entering the pipeline, before post-query filters.
            onRead: (doc) => tracker?.trackRead(getDocumentSize(doc)),
          });
        return JSON.stringify(
          convexToJson({
            page,
            isDone,
            continueCursor,
            splitCursor,
            pageStatus,
          }),
        );
      }
      case "1.0/insert": {
        const _id = db.insert(args.table, jsonToConvex(args.value));
        tracker?.trackWrite(getConvexSize(db.get(args.table, _id)));
        return JSON.stringify({ _id });
      }
      case "1.0/shallowMerge": {
        const { table, id, value } = args;
        getAndTrack(table, id);
        db.patch(table, id, value);
        tracker?.trackWrite(getConvexSize(db.get(table, id)));
        return JSON.stringify({});
      }
      case "1.0/replace": {
        const { table, id, value } = args;
        getAndTrack(table, id);
        db.replace(table, id, value);
        tracker?.trackWrite(getConvexSize(db.get(table, id)));
        return JSON.stringify({});
      }
      case "1.0/remove": {
        const { table, id } = args;
        getAndTrack(table, id);
        db.delete(table, id);
        tracker?.trackWrite(0);
        return JSON.stringify({});
      }
      case "1.0/getTransactionMetrics": {
        if (tracker) {
          return JSON.stringify(convexToJson(tracker.getTransactionMetrics()));
        }
        throw new Error(
          "getTransactionMetrics() can only be called from a query or mutation. " +
            "It is not available in actions or outside of a Convex function.",
        );
      }
      case "1.0/getFunctionMetadata": {
        const ctx = executionContextStorage.getStore();
        if (!ctx) {
          throw new Error(
            "getFunctionMetadata() can only be called from within a Convex function.",
          );
        }
        return JSON.stringify(
          convexToJson({
            name: ctx.udfPath,
            componentPath: ctx.componentPath,
          }),
        );
      }
      case "1.0/getRequestMetadata": {
        const ctx = executionContextStorage.getStore();
        const requestMetadata = requestMetadataStorage.getStore();
        if (ctx === undefined || requestMetadata === undefined) {
          throw new Error(
            "getRequestMetadata() can only be called from a mutation, action " +
              "or HTTP action. It is not available in queries or outside of a " +
              "Convex function.",
          );
        }
        if (ctx.udfType === "query") {
          throw new Error(
            "getRequestMetadata() can only be called from a mutation, action " +
              "or HTTP action. It is not available in queries.",
          );
        }
        return JSON.stringify(convexToJson(requestMetadata));
      }
      case "1.0/getDeploymentMetadata": {
        return JSON.stringify(
          convexToJson({
            name: "test",
            region: null,
            class: "s16",
          }),
        );
      }
      case "1.0/actions/query": {
        const { name, args: queryArgs } = args;
        return JSON.stringify(
          convexToJson(
            await withAuth().runQuery(makeFunctionReference(name), queryArgs),
          ),
        );
      }
      case "1.0/actions/mutation": {
        const { name, args: mutationArgs } = args;
        return JSON.stringify(
          convexToJson(
            await withAuth().runMutation(
              makeFunctionReference(name),
              mutationArgs,
            ),
          ),
        );
      }
      case "1.0/actions/action": {
        const { name, args: actionArgs } = args;
        return JSON.stringify(
          convexToJson(
            await withAuth().runAction(makeFunctionReference(name), actionArgs),
          ),
        );
      }
      case "1.0/runUdf": {
        const {
          udfType,
          name,
          reference,
          functionHandle,
          args: udfArgsJson,
          transactionLimits,
        } = args;
        if (executionContextStorage.getStore()?.udfType === "init") {
          throw new Error("Can't call another function from an init function.");
        }
        const udfArgs = jsonToConvex(udfArgsJson);
        const functionPath = getFunctionPathFromAddress({
          name,
          reference,
          functionHandle,
        });
        if (udfType === "query") {
          return JSON.stringify(
            convexToJson(
              await withAuth().queryFromPath(
                functionPath,
                udfArgs,
                transactionLimits,
              ),
            ),
          );
        }
        if (udfType === "snapshotQuery") {
          return JSON.stringify(
            convexToJson(
              await withAuth().snapshotQueryFromPath(
                functionPath,
                udfArgs,
                transactionLimits,
              ),
            ),
          );
        }
        if (udfType === "mutation") {
          return JSON.stringify(
            convexToJson(
              await withAuth().mutationFromPath(
                functionPath,
                udfArgs,
                transactionLimits,
              ),
            ),
          );
        }
        throw new Error(
          `\`convexTest\` does not support udf type: "${udfType}"`,
        );
      }
      case "1.0/createFunctionHandle": {
        const { name, reference, functionHandle } = args;
        const functionPath = getFunctionPathFromAddress({
          name,
          reference,
          functionHandle,
        });
        // Load the component eagerly so a missing registration fails here.
        getComponentInfo(functionPath.componentPath);
        const handle = createFunctionHandle(functionPath);
        return JSON.stringify(handle);
      }

      case "1.0/actions/schedule": {
        return await withAuth().run(async () => {
          return await getSyscalls().asyncSyscall("1.0/schedule", jsonArgs);
        });
      }
      case "1.0/schedule": {
        const {
          name,
          reference,
          functionHandle,
          args: fnArgs,
          ts: tsInSecs,
        } = args;
        const functionAddress = { name, reference, functionHandle };
        const functionPath = getFunctionPathFromAddress(functionAddress);
        // Load the component eagerly so a missing registration fails here.
        getComponentInfo(functionPath.componentPath);
        // Convert args to a Convex value before storing them or passing them to the function
        const parsedArgs = jsonToConvex(fnArgs);
        // Scheduled arguments cannot contain pending values
        if (hasPendingValues(parsedArgs)) {
          throw new Error(
            `Invalid arguments for "${functionPath.udfPath}": a scheduled ` +
              `function cannot take an unresolved placeholder value.`,
          );
        }

        tracker?.trackScheduledFunction(getConvexSize(parsedArgs));

        const jobId = db.insert("_scheduled_functions", {
          args: [parsedArgs],
          name: functionPath.udfPath,
          scheduledTime: tsInSecs * 1000,
          state: { kind: "pending" },
        });
        const componentPath = getCurrentComponentPath();
        // Capture the current test's ConvexGlobal so the scheduled
        // function callback (which runs via setTimeout, outside the
        // original ALS context) uses the correct test state.
        const capturedGlobal = getConvexGlobal();
        const scheduler = capturedGlobal.scheduler;
        // Queue the setTimeout outside any nested-mutation ALS context so
        // the scheduled function starts as a fresh top-level execution
        // rather than inheriting a stale parent lock.
        nestedTxStorage.exit(() => {
          scheduler.timerScheduled();
          frameworkSetTimeout(
            () => {
              scheduler.timerFired();
              const promise = convexGlobalStorage
                .run(capturedGlobal, () =>
                  runScheduledFunction({
                    db,
                    jobId,
                    componentPath,
                    functionPath,
                    args: parsedArgs,
                    name,
                  }),
                )
                .finally(() => {
                  scheduler.finish(promise);
                });
              scheduler.add(promise);
            },
            // Like the backend's scheduler, measure the due time against the
            // wall clock, not the scheduling transaction's pinned time.
            Math.max(0, tsInSecs * 1000 - frameworkNow()),
          );
        });
        return JSON.stringify(convexToJson(jobId));
      }
      case "1.0/actions/cancel_job": {
        // TODO: Match the remaining backend cancellation semantics: tasks
        // scheduled after cancellation, including through nested calls, should
        // start canceled. A canceled action should stay canceled if it later
        // throws, and canceling a terminal job should leave its state unchanged.
        await withAuth().run(async () => {
          // Actions may cancel their own job; only mutation cancellation
          // checks the scheduled request ID.
          db.patch("_scheduled_functions", args.id, {
            state: { kind: "canceled" },
          });
        });
        return JSON.stringify({});
      }
      case "1.0/actions/vectorSearch": {
        const {
          query: { indexName, limit, vector, expressions },
        } = args;
        const results = db.vectorSearch(
          indexName,
          vector,
          // Probably an unintentional implementation in Convex
          // where expressions is only a single expression
          expressions === null ? [] : [expressions],
          limit,
        );
        return JSON.stringify(convexToJson({ results }));
      }
      case "1.0/cancel_job": {
        const { id } = args;
        if (requestMetadataStorage.getStore()?.scheduledFunctionId === id) {
          // The backend gates this on the function's installed Convex version.
          const [major, minor] = version.split(".").map(Number);
          if (major > 1 || (major === 1 && minor >= 50)) {
            throw new Error(
              "ScheduledFunctionCancelingItself: A mutation cannot cancel itself",
            );
          }
          console.warn(
            "A scheduled mutation canceling itself is deprecated and will throw an error in a future version of Convex.",
          );
          return JSON.stringify(null);
        }
        db.patch("_scheduled_functions", id, { state: { kind: "canceled" } });
        return JSON.stringify({});
      }
      case "1.0/storageDelete": {
        const { storageId } = args;
        const metadata = db.get("_storage", storageId);
        if (metadata !== null && db.isPendingFile(storageId)) {
          tracker?.untrackFileWrite(metadata.size as number);
        }
        await writeToDatabase(async (db) => {
          db.delete("_storage", storageId);
        });
        return JSON.stringify({});
      }
      case "1.0/storageGetUrl": {
        const { storageId } = args;
        const metadata = db.get("_storage", storageId);
        if (metadata === null) {
          return JSON.stringify(null);
        }
        const { sha256 } = metadata;
        // In the real backend the URL ofc isn't the sha
        const url =
          "https://some-deployment.convex.cloud/api/storage/" +
          (sha256 as string);
        return JSON.stringify(convexToJson(url));
      }
      case "1.0/storageStore": {
        const { blob, contentType, sha256: expectedSha256 } = args;
        const bytes = Base64.toByteArray(blob) as Uint8Array<ArrayBuffer>;
        const type = contentType || undefined;
        const file = new Blob([bytes], { type });
        const sha256 = await blobSha(file);
        if (expectedSha256 != null && expectedSha256 !== sha256) {
          throw new Error(
            `Sha256 mismatch. Expected: ${expectedSha256} Actual: ${sha256}`,
          );
        }
        tracker?.trackFileWrite(bytes.length);
        const storageId = await writeToDatabase(async (db) => {
          const storageId = db.insert("_storage", {
            size: bytes.length,
            sha256,
            ...(type !== undefined ? { contentType: type } : {}),
          });
          db.storeFile(storageId, file);
          return storageId;
        });
        return JSON.stringify(convexToJson(storageId));
      }
      case "1.0/storageGet": {
        const { storageId } = args;
        const file = db.getFile(storageId);
        if (file == null) {
          return JSON.stringify(null);
        }
        const metadata = db.get("_storage", storageId)!;
        const fileKey = `${db.componentPath}:${storageId}`;
        const filesRead = getTransactionManager().getFilesRead();
        if (!db.isPendingFile(storageId) && !filesRead.has(fileKey)) {
          tracker?.trackFileRead(metadata.size as number);
          filesRead.add(fileKey);
        }
        return JSON.stringify({
          blob: Base64.fromByteArray(new Uint8Array(await file.arrayBuffer())),
          contentType: metadata.contentType ?? null,
        });
      }
      case "1.0/storageGenerateUploadUrl": {
        // In the real backend the token is cryptographically secure
        const url =
          "https://some-deployment.convex.cloud/api/storage/upload?token=" +
          Math.random();
        return JSON.stringify(convexToJson(url));
      }
      case "1.0/count": {
        const { table } = args;
        const queryId = db.startQuery({
          source: { type: "FullTableScan", tableName: table, order: "asc" },
          operators: [],
        });
        let count = 0;

        while (true) {
          const result = db.queryNext(queryId);
          if (result.done) {
            break;
          }
          count += 1;
        }
        return JSON.stringify(count);
      }
      case "1.0/auditLog": {
        return JSON.stringify(null);
      }
      case "1.0/createServiceToken":
      case "1.0/getServiceUrl": {
        const udfType = executionContextStorage.getStore()?.udfType;
        if (udfType !== "action" && udfType !== "http") {
          throw new Error(`Unknown async operation ${op}`);
        }
        const { service } = args;
        if (service !== "ai-gateway") {
          throw new Error(`Unsupported Convex service: ${String(service)}`);
        }
        if (op === "1.0/getServiceUrl") {
          return JSON.stringify("https://ai-gateway.convex.test");
        }
        // An unsecured, short-lived test credential cannot authenticate to a
        // real service. The reserved .test URL above avoids production endpoints.
        const iat = Math.floor(Date.now() / 1000);
        const header = base64UrlEncode(
          JSON.stringify({ alg: "none", typ: "JWT" }),
        );
        const payload = base64UrlEncode(
          JSON.stringify({
            iss: "convex-test",
            aud: service,
            iat,
            exp: iat + 300,
          }),
        );
        return JSON.stringify(`${header}.${payload}.`);
      }
      case "1.0/signJwt": {
        // Signing reads the current time, which query caching could serve
        // stale, so the backend only allows it in mutations. Actions don't
        // route this syscall at all.
        const udfType = executionContextStorage.getStore()?.udfType;
        if (udfType === "query") {
          throw new Error(
            "ctx.auth.signJwt() can't be called from a query. Call it from a mutation.",
          );
        }
        if (udfType !== "mutation" && udfType !== "init") {
          throw new Error(`Unknown async operation ${op}`);
        }
        return JSON.stringify(await signDeploymentJwt(args));
      }
      case "1.0/getDeploymentJwks": {
        // Allowed everywhere: the keys are deployment state, not a fresh
        // value like a token's `iat` and `exp`.
        return JSON.stringify(await deploymentJwks());
      }
      default: {
        throw new Error(
          `\`convexTest\` does not support async syscall: "${op}"`,
        );
      }
    }
  };
}

function jsSyscallImpl() {
  return async (op: string, args: Record<string, any>): Promise<any> => {
    const db = getDb();
    switch (op) {
      case "storage/storeBlob": {
        const { blob } = args as { blob: Blob };
        return await writeToDatabase(async (db) => {
          const storageId = db.insert("_storage", {
            size: blob.size,
            sha256: await blobSha(blob),
            ...(blob.type ? { contentType: blob.type } : {}),
          });
          db.storeFile(storageId, blob);
          return storageId;
        });
      }
      case "storage/getBlob": {
        const { storageId } = args as { storageId: GenericId<"_storage"> };
        return db.getFile(storageId);
      }
      default: {
        throw new Error(`\`convexTest\` does not support js syscall: "${op}"`);
      }
    }
  };
}

// If we're in action, wrap the write in a transaction.
async function writeToDatabase<T>(impl: (db: DatabaseFake) => Promise<T>) {
  const db = getDb();
  if (!getTransactionManager().isInCurrentTransaction()) {
    return await withAuth().run(async () => {
      return await impl(db);
    });
  } else {
    return await impl(db);
  }
}

async function blobSha(blob: Blob) {
  const arrayBuffer = await blob.arrayBuffer();
  const hashBuffer = await crypto.subtle.digest("SHA-256", arrayBuffer);
  const hashArray = new Uint8Array(hashBuffer);
  return btoa(String.fromCharCode(...hashArray));
}

export type TestConvex<SchemaDef extends SchemaDefinition<any, boolean>> =
  TestConvexRoot<DataModelFromSchemaDefinition<SchemaDef>>;

/**
 * A component registration function that can return its generated `ComponentApi`.
 *
 * Returning typed component references lets `defineTestApp` infer the component
 * API. If the component doesn't export a `register` function from their /test
 * entrypoint that returns the ComponentApi, it can be cast like
 * `{ register: componentTest.register as ComponentRegistration<ComponentApi> }`.
 *
 * Note: `defineTestApp` does not use the returned value.
 */
export type ComponentRegistration<Api> = (
  t: TestConvexRoot<any>,
  name: string,
) => Api | void;

// Match generated ComponentApi<Name> references for each named installation.
type ComponentApiForName<Api, Name extends string> =
  Api extends FunctionReference<
    infer Type,
    infer Visibility,
    infer Args,
    infer ReturnType
  >
    ? FunctionReference<Type, Visibility, Args, ReturnType, Name>
    : { [Key in keyof Api]: ComponentApiForName<Api[Key], Name> };

type ComponentRegistrations = Record<
  string,
  { register: ComponentRegistration<any> }
>;

type RegisteredComponents<Components extends ComponentRegistrations> = {
  [
    Name in keyof Components
  ]: Components[Name]["register"] extends ComponentRegistration<infer Api>
    ? ComponentApiForName<Api, Name & string>
    : never;
};

/** Schema-bound function builders for an in-memory test application. */
export type TestApp<
  SchemaDef extends SchemaDefinition<any, boolean>,
  Components extends ComponentRegistrations = Record<never, never>,
> = {
  /** Typed references to the configured component instances. */
  components: RegisteredComponents<Components>;
  query: QueryBuilder<DataModelFromSchemaDefinition<SchemaDef>, "public">;
  internalQuery: QueryBuilder<
    DataModelFromSchemaDefinition<SchemaDef>,
    "internal"
  >;
  mutation: MutationBuilder<DataModelFromSchemaDefinition<SchemaDef>, "public">;
  internalMutation: MutationBuilder<
    DataModelFromSchemaDefinition<SchemaDef>,
    "internal"
  >;
  action: ActionBuilder<DataModelFromSchemaDefinition<SchemaDef>, "public">;
  internalAction: ActionBuilder<
    DataModelFromSchemaDefinition<SchemaDef>,
    "internal"
  >;
  /**
   * Define function modules by their paths, e.g. `test` or `test/callbacks`,
   * without a leading `./` or file extension. Values are module exports,
   * rather than the import functions returned by `import.meta.glob`.
   *
   * References have the same types and behavior as generated Convex APIs.
   * Public functions appear in `api` and internal functions in `internal`.
   * Handlers referencing this API may need explicit return type annotations
   * to break TypeScript inference cycles.
   */
  defineModules<Modules extends Record<string, object>>(
    this: void,
    modules: Modules,
  ): TestAppDefinition<SchemaDef, Modules>;
};

/** Typed function references and a factory for fresh test instances. */
export type TestAppDefinition<
  SchemaDef extends SchemaDefinition<any, boolean>,
  Modules extends Record<string, object>,
> = {
  api: FilterApi<ApiFromModules<Modules>, FunctionReference<any, "public">>;
  internal: FilterApi<
    ApiFromModules<Modules>,
    FunctionReference<any, "internal">
  >;
  /**
   * Create fresh test state and register the configured components. Additional
   * components can be registered using their helpers or `t.registerComponent`.
   * `transactionLimits` has the same meaning as in `convexTest`.
   */
  createTest(
    this: void,
    options?: { transactionLimits?: Partial<TransactionMetrics> | boolean },
  ): TestConvex<SchemaDef>;
};

/**
 * Define an in-memory test application without generated files.
 *
 * The returned function builders are typed against `schema`. Pass modules of
 * registered functions to `defineModules` to get typed `api` and `internal`
 * references and a `createTest` factory. Each call to `createTest` creates fresh
 * test state and returns a normal {@link TestConvex}, which can be passed to
 * component testing helpers such as `componentTest.register(t)`.
 *
 * Optional `components` map keys are instance names. Each helper's `register`
 * function runs on every `createTest()` call. Helpers returning `ComponentApi`
 * provide typed references in `app.components`. Helpers returning `void` can be
 * cast to {@link ComponentRegistration} to supply their API type.
 *
 * @example
 * const app = defineTestApp({
 *   schema,
 *   components: { sampleComponent: sampleComponentTest },
 * });
 * const { api, createTest } = app.defineModules({
 *   test: {
 *     bar: app.mutation({
 *       args: { text: v.string() },
 *       returns: v.id("messages"),
 *       handler: async (ctx, args) => ctx.db.insert("messages", args),
 *     }),
 *   },
 * });
 * const t = createTest();
 * await t.mutation(api.test.bar, { text: "hello" });
 * await t.query(app.components.sampleComponent.public.count, { name: "beans" });
 */
export function defineTestApp<
  SchemaDef extends SchemaDefinition<any, boolean>,
  Components extends ComponentRegistrations = Record<never, never>,
>({
  schema,
  components,
}: {
  schema: SchemaDef;
  components?: Components;
}): TestApp<SchemaDef, Components> {
  const registrations = Object.entries(components ?? {});
  for (const [name] of registrations) {
    if (name.length === 0 || name.includes("/") || name.includes(" ")) {
      throw new Error(
        `Invalid component instance name "${name}": expected a nonempty name without slashes or spaces. Register nested components inside the parent's "register" function.`,
      );
    }
  }
  return {
    components: componentsGeneric() as RegisteredComponents<Components>,
    query: queryGeneric,
    internalQuery: internalQueryGeneric,
    mutation: mutationGeneric,
    internalMutation: internalMutationGeneric,
    action: actionGeneric,
    internalAction: internalActionGeneric,

    defineModules<Modules extends Record<string, object>>(modules: Modules) {
      const moduleMap = new Map(Object.entries(modules));
      const loadModule = moduleLoader(
        async (path: string) => {
          const module = moduleMap.get(path);
          if (module === undefined) {
            throw new Error(`Could not find module for: "${path}"`);
          }
          return module;
        },
        [...moduleMap.keys()],
      );
      const fullApi = anyApi as unknown as ApiFromModules<Modules>;
      return {
        api: filterApi<typeof fullApi, FunctionReference<any, "public">>(
          fullApi,
        ),
        internal: filterApi<typeof fullApi, FunctionReference<any, "internal">>(
          fullApi,
        ),
        createTest(
          options: {
            transactionLimits?: Partial<TransactionMetrics> | boolean;
          } = {},
        ): TestConvex<SchemaDef> {
          const t = createConvexTest(
            schema,
            loadModule,
            options.transactionLimits ?? false,
          );
          for (const [name, registration] of registrations) {
            const result = registration.register(t, name);
            if (typeof result?.then === "function") {
              throw new Error(
                `Component registration for "${name}" returned a Promise. ` +
                  `The "register" function must be synchronous.`,
              );
            }
          }
          return t;
        },
      };
    },
  };
}

export type TestConvexForDataModel<DataModel extends GenericDataModel> = {
  /**
   * Call a public or internal query, or run an inline query function.
   *
   * @param query A {@link FunctionReference} for the query, or an inline
   *   async function that receives a query context.
   * @param args  An arguments object for the query. If this is omitted,
   *   the arguments will be `{}`. Not used for inline functions.
   * @returns A `Promise` of the query's result.
   */
  query: <
    QueryOrFunc extends
      | FunctionReference<"query", any>
      | FunctionReference_future<"query", any>
      | ((ctx: GenericQueryCtx<DataModel>) => Promise<any>),
  >(
    queryOrFunc: QueryOrFunc,
    ...args: QueryOrFunc extends
      FunctionReference<"query", any> | FunctionReference_future<"query", any>
      ? OptionalRestArgs<QueryOrFunc>
      : []
  ) => Promise<
    QueryOrFunc extends
      FunctionReference<"query", any> | FunctionReference_future<"query", any>
      ? FunctionReturnType<QueryOrFunc>
      : QueryOrFunc extends (ctx: any) => Promise<infer Output>
        ? Output
        : never
  >;

  /**
   * Call a public or internal mutation, or run an inline mutation function.
   *
   * @param mutation A {@link FunctionReference} for the mutation, or an inline
   *   async function that receives a mutation context.
   * @param args  An arguments object for the mutation. If this is omitted,
   *   the arguments will be `{}`. Not used for inline functions.
   * @returns A `Promise` of the mutation's result.
   */
  mutation: <
    MutationOrFunc extends
      | FunctionReference<"mutation", any>
      | FunctionReference_future<"mutation", any>
      | ((ctx: GenericMutationCtx<DataModel>) => Promise<any>),
  >(
    mutationOrFunc: MutationOrFunc,
    ...args: MutationOrFunc extends
      | FunctionReference<"mutation", any>
      | FunctionReference_future<"mutation", any>
      ? OptionalRestArgs<MutationOrFunc>
      : []
  ) => Promise<
    MutationOrFunc extends
      | FunctionReference<"mutation", any>
      | FunctionReference_future<"mutation", any>
      ? FunctionReturnType<MutationOrFunc>
      : MutationOrFunc extends (ctx: any) => Promise<infer Output>
        ? Output
        : never
  >;

  /**
   * Call a public or internal action, or run an inline action function.
   *
   * @param action A {@link FunctionReference} for the action, or an inline
   *   async function that receives an action context.
   * @param args  An arguments object for the action. If this is omitted,
   *   the arguments will be `{}`. Not used for inline functions.
   * @returns A `Promise` of the action's result.
   */
  action: <
    ActionOrFunc extends
      | FunctionReference<"action", any>
      | FunctionReference_future<"action", any>
      | ((ctx: GenericActionCtx<DataModel>) => Promise<any>),
  >(
    actionOrFunc: ActionOrFunc,
    ...args: ActionOrFunc extends
      FunctionReference<"action", any> | FunctionReference_future<"action", any>
      ? OptionalRestArgs<ActionOrFunc>
      : []
  ) => Promise<
    ActionOrFunc extends
      FunctionReference<"action", any> | FunctionReference_future<"action", any>
      ? FunctionReturnType<ActionOrFunc>
      : ActionOrFunc extends (ctx: any) => Promise<infer Output>
        ? Output
        : never
  >;

  /**
   * Read from and write to the mock backend.
   *
   * The callback runs in a transaction, so `fetch` and timers are unavailable.
   * Perform setup that needs them in the test body before calling `run`.
   *
   * @param func The async function that reads or writes to the mock backend.
   *   It receives a ctx as its first argument that conforms to
   *   {@link GenericMutationCtx},
   *   so it can be passed to functions that expect either context, use file
   *   storage, etc.
   * @returns A `Promise` of the function's result.
   */
  run: <Output>(
    func: (
      ctx: GenericMutationCtx<DataModel> &
        Pick<
          GenericActionCtx<DataModel>,
          "storage"
          // ActionCtx support was added in 0.0.40, removed in 0.0.41, should be added back in the future.
          // | "runAction"
          // | "vectorSearch"
        >,
    ) => Promise<Output>,
  ) => Promise<Output>;

  /**
   * Call an HTTP action.
   *
   * @param path The request URL's path and optional query and fragment.
   * @param init Standard `fetch` options.
   */
  fetch(pathQueryFragment: string, init?: RequestInit): Promise<Response>;

  /**
   * Wait for all scheduled functions currently in the "inProgress" state
   * to either finish successfully or fail.
   *
   * Use in combination with `vi.useFakeTimers()` and `vi.runAllTimers()`
   * to control precisely the execution of scheduled functions.
   *
   * Typically:
   * 1. Use `vi.runAllTimers()` or similar to advance
   *   time such that a function is scheduled.
   * 2. Use `finishInProgressScheduledFunctions()` to wait for the function
   *    to finish.
   */
  finishInProgressScheduledFunctions: () => Promise<void>;

  /**
   * Wait for all currently scheduled functions and any functions they
   * schedule to either finish successfully or fail.
   *
   * Use in combination with `vi.useFakeTimers()` to test scheduled functions.
   *
   * Fake timers only need to be active for this call: functions scheduled
   * while real timers were active are drained too, as long as their
   * scheduled time has already passed on the real clock (e.g. scheduled
   * with `runAfter(0)`). Ones whose real-clock time hasn't arrived are
   * skipped, since they cannot be forced to fire by advancing fake timers.
   *
   * @param advanceTimers Function that advances timers,
   *   usually `vi.runAllTimers`. This function will be called in a loop
   *   with `finishInProgressScheduledFunctions()`.
   * @param maxIterations Optional maximum number of iterations to run
   *   scheduled functions. If not provided, defaults to 100. This prevents
   *   infinite loops if scheduled functions keep scheduling more functions.
   */
  finishAllScheduledFunctions: (
    advanceTimers: () => void,
    maxIterations?: number,
  ) => Promise<void>;

  /**
   * To test functions which depend on the current authenticated user identity
   * you can create a version of the `t` accessor with given user identity
   * attributes.
   *
   * The instance this is called on is not affected, and it is possible to call
   * `withIdentity` on the returned value again to call functions with a new identity.
   *
   * @param identity A subset of {@link UserIdentity} attributes. If you
   *   don't provide `issuer`, `subject` or `tokenIdentifier` they are
   *   generated automatically.
   */
  withIdentity(
    identity: Partial<UserIdentity>,
  ): TestConvexForDataModel<DataModel>;
  /**
   * To test functions which depend on the metadata of the request that
   * triggered them, available via `ctx.meta.getRequestMetadata()`, you can
   * create a version of the `t` accessor with a given IP address and user
   * agent.
   *
   * Attributes that aren't provided are `null`, and calling this method again
   * replaces both of them.
   *
   * @param metadata The `ip` and `userAgent` of the request.
   */
  withRequestMetadata(
    metadata: Partial<Pick<RequestMetadata, "ip" | "userAgent">>,
  ): TestConvexForDataModel<DataModel>;

  /**
   * The public keys that verify tokens signed by `ctx.auth.signJwt`, like the
   * JWKS the backend serves at `${CONVEX_CLOUD_URL}/.well-known/jwks.json`.
   *
   * Each `convexTest` instance signs with its own key, so the JWKS only
   * verifies tokens signed by the same instance.
   *
   * ```ts
   * import { createLocalJWKSet, jwtVerify } from "jose";
   *
   * const { payload } = await jwtVerify(token, createLocalJWKSet(await t.jwks()));
   * ```
   */
  jwks(): Promise<DeploymentJwks>;
};

/**
 * A JSON Web Key Set, as returned by {@link TestConvexForDataModel.jwks}.
 */
export type DeploymentJwks = { keys: DeploymentJwk[] };

/**
 * A public key that verifies tokens signed by `ctx.auth.signJwt`.
 */
export type DeploymentJwk = {
  kty: "EC";
  crv: "P-256";
  x: string;
  y: string;
  kid: string;
  alg: "ES256";
  use: "sig";
};

/**
 * The accessor returned by {@link convexTest}. In addition to running
 * functions, it can register components.
 */
export type TestConvexRoot<DataModel extends GenericDataModel> = {
  /**
   * Register a component, so that the functions of the app under test can call
   * it.
   *
   * @returns References to the component's functions, typed as `ComponentApi`.
   */
  registerComponent: <ComponentApi = unknown>(
    componentPath: string,
    schema: SchemaDefinition<GenericSchema, boolean>,
    glob: Record<string, () => Promise<any>>,
    options?: {
      /**
       * Arguments for the component's init, in place of the ones its parent's
       * `convex.config` passes. Function references are resolved in the
       * parent, as `app.use(component, { args })` does.
       */
      args?: Record<string, unknown>;
    },
  ) => ComponentApi;

  /**
   * Run the init of every registered component, as each push does.
   *
   * Inits of newly registered components also run before the next function
   * call, so call this only to run them again (inits must be idempotent) or
   * to check how one fails.
   */
  runInits: () => Promise<void>;
} & TestConvexForDataModel<DataModel>;

/**
 * @deprecated Use {@link TestConvexRoot} instead.
 */
export type TestConvexForDataModelAndIdentity<
  DataModel extends GenericDataModel,
> = TestConvexRoot<DataModel>;

function getComponentInfo(componentPath: string): ComponentInfo {
  const convex = getConvexGlobal();
  if (convex.components[componentPath] === undefined) {
    throw new Error(formatUnregisteredComponentError(componentPath));
  }
  return convex.components[componentPath];
}

function formatUnregisteredComponentError(componentPath: string): string {
  const convex = getConvexGlobal();
  const registered = Object.keys(convex.components).filter(
    (p) => p !== ROOT_COMPONENT_PATH,
  );
  const registeredHint =
    registered.length === 0
      ? "No components have been registered on this `convexTest` instance."
      : `Registered components: ${registered.map((p) => `"${p}"`).join(", ")}.`;
  return (
    `Component "${componentPath}" is not registered on this \`convexTest\` instance. ` +
    `If it is a published component, import its test helper and call it on \`t\` ` +
    `(e.g. \`import { register } from "<component>/test"; register(t);\`). ` +
    `Otherwise call \`t.registerComponent("${componentPath}", componentSchema, componentModules)\` directly ` +
    `(see https://docs.convex.dev/components/using#testing-components). ` +
    registeredHint
  );
}

function getDbForComponent(componentPath: string) {
  return getComponentInfo(componentPath).db;
}

function getDb() {
  return getDbForComponent(getCurrentComponentPath());
}

function getTransactionManager() {
  return getConvexGlobal().transactionManager;
}

function getCurrentComponentPath() {
  return (
    executionContextStorage.getStore()?.componentPath ?? ROOT_COMPONENT_PATH
  );
}

function getModules() {
  return getModulesForComponent(getCurrentComponentPath());
}

function getModulesForComponent(componentPath: string) {
  return getComponentInfo(componentPath).modules;
}

function getSyscalls() {
  return getConvexGlobal() as {
    syscall: ReturnType<typeof syscallImpl>;
    asyncSyscall: ReturnType<typeof asyncSyscallImpl>;
  };
}

function moduleCache(specifiedModules?: Record<string, () => Promise<any>>) {
  const modules = specifiedModules ?? import.meta.glob("./convex/**/*.*s");
  const prefix = findModulesRoot(
    Object.keys(modules),
    specifiedModules !== undefined,
  );
  const modulesWithoutExtension = Object.fromEntries(
    Object.entries(modules).map(([path, module]) => [
      path.replace(/\.[^.]+$/, ""),
      module,
    ]),
  );
  const load = async (path: string) => {
    const module = modulesWithoutExtension[prefix + path];
    if (module === undefined) {
      throw new Error(`Could not find module for: "${path}"`);
    }
    return await module();
  };
  const paths = Object.keys(modulesWithoutExtension)
    .filter((path) => path.startsWith(prefix))
    .map((path) => path.slice(prefix.length));
  return moduleLoader(load, paths);
}

type ModuleLoader = ((path: string) => Promise<any>) & {
  // Module paths relative to the modules root, without extensions.
  paths: string[];
};

function moduleLoader(
  load: (path: string) => Promise<any>,
  paths: string[],
): ModuleLoader {
  return Object.assign(load, { paths });
}

function findModulesRoot(modulesPaths: string[], userProvidedModules: boolean) {
  const generatedFilePath = modulesPaths.find((path) =>
    path.includes("_generated"),
  );
  if (generatedFilePath !== undefined) {
    return generatedFilePath.split("_generated", 2)[0];
  }

  throw new Error(
    'Could not find the "_generated" directory, make sure to run ' +
      "`npx convex dev` or `npx convex codegen`. " +
      (userProvidedModules
        ? "Make sure your `import.meta.glob` includes the files in the " +
          '"_generated" directory'
        : "If your Convex functions aren't defined in a directory " +
          'called "convex" sibling to your node_modules, ' +
          "provide the second argument to `convexTest`"),
  );
}

type ComponentInfo = {
  db: DatabaseFake;
  modules: ReturnType<typeof moduleCache>;
};

type FunctionPath = {
  // e.g. "aggregate"
  componentPath: string;
  // e.g. "messages:list"
  udfPath: string;
};

// "init" is a component's init: a mutation run by a push, which the runtime
// restricts further (see `runMutationWithHandler`).
type UdfType = "query" | "mutation" | "init" | "action" | "http";

type ExecutionContext = {
  componentPath: string;
  udfPath: string;
  udfType: UdfType;
  depth: number; // 0 = top-level, 1+ = nested
  // Queries share this counter with their nested queries, including components.
  // Mutations have no counter; each query they call gets a fresh one.
  pagination?: { calls: number };
};

const executionContextStorage = new AsyncLocalStorage<ExecutionContext>();

type RequestMetadataFake = {
  ip: string | null;
  userAgent: string | null;
  requestId: string;
  scheduledFunctionId: string | null;
  authToken: string | null;
};

// The request metadata of the innermost top-level function execution. Nested
// calls (including calls into components) inherit it, so it is only created
// when a function is called from the test itself, and replaced when a
// scheduled function starts.
const requestMetadataStorage = new AsyncLocalStorage<RequestMetadataFake>();

// The request attributes a test accessor was configured with, which only
// apply to the functions the accessor itself calls.
type RequestOptions = { ip: string | null; userAgent: string | null };

const DEFAULT_REQUEST_OPTIONS: RequestOptions = { ip: null, userAgent: null };

// Request IDs are internal bookkeeping. Generating them must not consume a
// handler's mocked randomness, so they come from a counter instead of `crypto`
// or `Math.random`. The prefix marks these as test-only IDs.
let nextRequestId = 0;

function newRequestId() {
  return `fake-request-id-${nextRequestId++}`;
}

type ConvexGlobal = {
  components: Record<string, ComponentInfo>;
  transactionManager: TransactionManager;
  scheduler: Scheduler;
  syscall: (op: string, args: any) => any;
  asyncSyscall: (op: string, args: any) => Promise<any>;
  jsSyscall: (op: string, args: any) => Promise<any>;
  // Generated on the first `ctx.auth.signJwt` call.
  deploymentJwtKey: Promise<DeploymentJwtKey> | null;
  // Arguments for a component's init, by component path, that replace the
  // ones its parent's `convex.config` passes.
  componentArgs: Record<string, Record<string, unknown>>;
  // Components registered since the last successful push, whose inits run
  // before the next function call.
  uninitializedComponents: Set<string>;
};

type DeploymentJwtKey = {
  privateKey: CryptoKey;
  kid: string;
  publicJwk: DeploymentJwk;
};

function getConvexGlobal(): ConvexGlobal {
  const store = convexGlobalStorage.getStore();
  if (!store) {
    throw new Error(
      "No active convexTest context. " +
        "Make sure you are calling methods on the object returned by convexTest().",
    );
  }
  return store;
}

/*
 * Globals that libraries (e.g. workflow engines) may override during a handler.
 * Each invocation gets its own AsyncLocalStorage context. Nested Convex calls
 * start from the test environment's globals, with their own runtime restrictions,
 * instead of inheriting the caller's overrides.
 *
 * Known limitation: isolation relies on the getters/setters installed below:
 * ✅ `globalThis.Math = replacement` calls the setter, isolating the replacement.
 * ❌ `delete globalThis.crypto` removes the getter/setter entirely.
 * ❌ `Object.defineProperty(globalThis, "Math", { value: replacement })` replaces
 *    the getter/setter without calling it, changing the global for all handlers.
 * ❌ `Math.random = replacement` changes a property of the shared Math object,
 *    without calling the setter for globalThis.Math.
 *
 * At handler entry we rewrap globals replaced by test setup (including
 * vi.stubGlobal), using the replacement as the shared default. Replacing or
 * deleting a property while a handler is running bypasses isolation, including
 * when another test changes its shared mocks.
 *
 * To work around this limitation, assign a replacement object to the global.
 * To make a global's value unavailable, assign undefined instead of using delete
 * (the property remains present). Only the globals listed below that are present
 * and configurable in the test environment can be isolated. Assignments outside
 * handlers change the shared test environment and must be restored by the test.
 */
const PATCHABLE_GLOBALS = [
  "setTimeout",
  "clearTimeout",
  "setInterval",
  "clearInterval",
  "fetch",
  "Date",
  "Math",
  "console",
  "process",
  "crypto",
  "Crypto",
  "CryptoKey",
  "SubtleCrypto",
  "atob",
  "btoa",
  "Request",
  "Response",
  "Headers",
  "URL",
  "URLSearchParams",
  "AbortController",
  "TextEncoder",
  "TextDecoder",
  "structuredClone",
  "queueMicrotask",
  "performance",
] as const;

type GlobalOverrides = Record<string, unknown>;
const globalOverridesStorage = new AsyncLocalStorage<GlobalOverrides>();

// Framework clocks honor test setup (including fake timers), but not a
// handler's pinned Date or its own replacement of that global.
function frameworkNow(): number {
  return globalOverridesStorage.exit(() => Date.now());
}

// The framework's own scheduler needs setTimeout even when running inside a
// transaction context where setTimeout is disallowed for user code. Exit the
// override store before reading globalThis.setTimeout so we get the underlying
// value (which honors vitest fake timers, since those replace the proxy
// target rather than entering an ALS context). Registering the timer outside
// the store also means the callback doesn't inherit the scheduling function's
// overrides. Unlike `realSetTimeout`, this is fake-timer aware, so
// scheduled functions fire when a test advances timers.
function frameworkSetTimeout(
  cb: (...args: unknown[]) => void,
  ms?: number,
): ReturnType<typeof setTimeout> {
  return globalOverridesStorage.exit(() => globalThis.setTimeout(cb, ms));
}

// Globals that the real Convex runtime does not provide inside queries and
// mutations (transactions). Reading them is fine; calling them throws. Handlers
// can assign to `globalThis.fetch` etc. — writes land
// in the per-call ALS override map and replace the sentinel for that call.
function disallowedInTransaction(name: string): (...args: unknown[]) => never {
  return () => {
    throw new Error(
      `\`${name}\` is not supported in Convex queries or mutations. ` +
        `Move this code to an action; a mutation can start one with ` +
        `\`ctx.scheduler.runAfter(0, ...)\`.`,
    );
  };
}

function transactionGlobalOverrides(): GlobalOverrides {
  const BaseDate = globalOverridesStorage.exit(() => Date);
  const time = getTransactionManager().getTime();
  const now = () => time;
  return {
    Date: new Proxy(BaseDate, {
      get(target, property, receiver) {
        return property === "now"
          ? now
          : Reflect.get(target, property, receiver);
      },
      construct(target, args, newTarget) {
        return Reflect.construct(
          target,
          args.length === 0 ? [time] : args,
          newTarget,
        );
      },
      apply() {
        return new BaseDate(time).toString();
      },
    }),
    fetch: disallowedInTransaction("fetch"),
    setTimeout: disallowedInTransaction("setTimeout"),
    clearTimeout: disallowedInTransaction("clearTimeout"),
    setInterval: disallowedInTransaction("setInterval"),
    clearInterval: disallowedInTransaction("clearInterval"),
  };
}

// Remember every installed getter/setter pair, including older pairs that
// vi.unstubAllGlobals may restore. Weak references let unused pairs be collected.
const globalProxySetters = new WeakMap<
  () => unknown,
  (value: unknown) => void
>();

// Rewrap tracked globals replaced by test setup, preserving the replacement as
// the default outside handlers. Keep these accessors installed between calls:
// removing them when one async handler finishes would affect overlapping calls.
function installGlobalProxies() {
  const g = globalThis as Record<string, unknown>;

  for (const key of PATCHABLE_GLOBALS) {
    if (!(key in g)) continue;
    const descriptor = Object.getOwnPropertyDescriptor(g, key);
    const installedSetter =
      // eslint-disable-next-line @typescript-eslint/unbound-method -- Compare accessor identity without calling it.
      descriptor?.get && globalProxySetters.get(descriptor.get);
    if (installedSetter && installedSetter === descriptor.set) continue;
    // Some environments or test setups make globals non-configurable.
    // Those properties cannot be replaced with our accessors, even if writable.
    if (descriptor?.configurable === false) continue;

    // Resolve inherited properties without evaluating getters during installation.
    let originalDescriptor = descriptor;
    for (
      let prototype = Object.getPrototypeOf(g);
      !originalDescriptor && prototype;
      prototype = Object.getPrototypeOf(prototype)
    ) {
      originalDescriptor = Object.getOwnPropertyDescriptor(prototype, key);
    }
    if (!originalDescriptor) continue;

    // Each installation owns a descriptor copy. Data properties use its value
    // as mutable backing state; accessors delegate on each read/write with the
    // global receiver and the reader's current context. Never read g[key] from
    // our getter, which would recurse. Keeping older copies separate also lets
    // vi.unstubAllGlobals restore the default from before a stub was installed.
    const defaultDescriptor = originalDescriptor;
    const get = () => {
      const store = globalOverridesStorage.getStore();
      if (store && key in store) return store[key];
      return "value" in defaultDescriptor
        ? defaultDescriptor.value
        : defaultDescriptor.get?.call(g);
    };
    const set = (value: unknown) => {
      const store = globalOverridesStorage.getStore();
      if (store) {
        store[key] = value;
      } else if ("value" in defaultDescriptor && defaultDescriptor.writable) {
        defaultDescriptor.value = value;
      } else if (defaultDescriptor.set) {
        defaultDescriptor.set.call(g, value);
      } else {
        throw new TypeError(`Cannot assign to read only global '${key}'`);
      }
    };

    try {
      Object.defineProperty(g, key, {
        get,
        set,
        configurable: true,
        enumerable: descriptor?.enumerable ?? false,
      });
      globalProxySetters.set(get, set);
    } catch {
      // Some environments may prevent replacing a global property.
    }
  }
}

/**
 * We use this wrapper every time we run user-provided code. Even though we
 * already install global proxies when `convexTest()` is first called, they can
 * be removed by changes we can't intercept (`delete`, `Object.defineProperty`,
 * `vi.stubGlobal`…). Re-installing them before every handler repairs isolation
 * after such changes.
 */
function runWithGlobalOverrides<T>(
  overrides: GlobalOverrides,
  handler: () => T,
) {
  installGlobalProxies();
  return globalOverridesStorage.run(overrides, handler);
}

// Install a permanent global proxy so the Convex runtime's
// `Convex.syscall` / `Convex.asyncSyscall` / `Convex.jsSyscall`
// always delegate to the current test's ALS context.
// This avoids writing per-test state to the global.
let _globalProxyInstalled = false;
function ensureGlobalProxy() {
  if (_globalProxyInstalled) return;
  _globalProxyInstalled = true;
  installGlobalProxies();
  (global as unknown as { Convex: ConvexGlobal }).Convex = {
    get syscall() {
      return getConvexGlobal().syscall;
    },
    get asyncSyscall() {
      return getConvexGlobal().asyncSyscall;
    },
    get jsSyscall() {
      return getConvexGlobal().jsSyscall;
    },
  } as ConvexGlobal;
}

// Per-test scheduler state: tracks executions of scheduled functions that
// have been fired (their setTimeout callback has run) but haven't yet
// settled.
class Scheduler {
  private _inFlight: Set<Promise<void>> = new Set();
  // Number of setTimeouts registered for scheduled functions that haven't
  // fired yet. Timers registered while real timers were active cannot be
  // fired by advancing fake timers — they need real event loop turns —
  // so `finishAllScheduledFunctions` uses this to know whether to keep
  // yielding instead of returning early.
  private _pendingTimerCount = 0;

  timerScheduled() {
    this._pendingTimerCount += 1;
  }

  timerFired() {
    this._pendingTimerCount -= 1;
  }

  anyPendingTimers(): boolean {
    return this._pendingTimerCount > 0;
  }

  add(promise: Promise<void>) {
    this._inFlight.add(promise);
  }

  finish(promise: Promise<void>) {
    this._inFlight.delete(promise);
  }

  anyFunctionsRunning(): boolean {
    return this._inFlight.size > 0;
  }

  async finishInProgressScheduledFunctions(): Promise<void> {
    while (this._inFlight.size > 0) {
      await Promise.all(this._inFlight);
    }
  }
}

class TransactionManager {
  // The DatabaseFake is used in the Convex global,
  // and so it restricts `convexTest` to run one function
  // at a time.
  // We force sequential execution to make sure actions
  // can run mutations in parallel.
  private _waitOnCurrentFunction: Promise<void> | null = null;
  private _markTransactionDone: (() => void) | null = null;
  // Tracks bandwidth usage and enforces limits for the current top-level
  // transaction (null when none is in progress). Like the per-component
  // `DatabaseFake`, it owns a stack of nested transaction layers; we drive its
  // `startTransaction` / `commit` / `rollback` in lockstep with the databases'.
  private _metricsTracker: TransactionMetricsTracker | null = null;
  private _limitsConfig: Partial<TransactionMetrics> | boolean;
  // The latest committed transaction's timestamp, in Unix nanoseconds.
  // Zero represents the initial empty database, before any commits.
  private _lastCommitTs: bigint = 0n;
  // Milliseconds pinned at top-level transaction start, shared by nested calls.
  private _udfTime: number | null = null;
  // Files already read by the current top-level transaction, including by
  // nested calls that rolled back. Reading one again doesn't count toward the
  // read limits.
  private _filesRead: Set<string> | null = null;

  constructor(limitsConfig: Partial<TransactionMetrics> | boolean = false) {
    this._limitsConfig = limitsConfig;
  }

  async begin(
    isNested: boolean,
    transactionLimits?: Partial<TransactionMetrics>,
  ) {
    // Take a lock only for the top-level of each transaction.
    // Nested transactions are not isolated so if you `Promise.all` on multiple
    // `ctx.runMutation` or `ctx.runQuery` calls, they won't be serialized.
    if (!isNested) {
      // Note the loop is important, as the current promise might resolve and a
      // new transaction could start before we get woken up.
      // This is the standard pattern for condition variables:
      // https://en.wikipedia.org/wiki/Monitor_(synchronization)
      while (this._waitOnCurrentFunction !== null) {
        await this._waitOnCurrentFunction;
      }
      // Round up in bigint arithmetic so a one-nanosecond commit clock tick
      // cannot disappear when converted to milliseconds.
      this._udfTime = Math.max(
        frameworkNow(),
        Number((this._lastCommitTs + 999_999n) / 1_000_000n),
      );
      this._waitOnCurrentFunction = new Promise((resolve) => {
        this._markTransactionDone = resolve;
      });
      this._metricsTracker = new TransactionMetricsTracker(this._limitsConfig);
      this._filesRead = new Set();
    }
    const convex = getConvexGlobal();
    for (const component of Object.values(convex.components)) {
      component.db.startTransaction();
    }
    this._metricsTracker?.startTransaction(transactionLimits);
  }

  // Used to distinguish between mutation and action execution
  // environment.
  isInTransaction() {
    return this._waitOnCurrentFunction !== null;
  }

  isInCurrentTransaction(): boolean {
    for (
      let lock = nestedTxStorage.getStore();
      lock !== undefined;
      lock = lock.parent
    ) {
      if (lock.transactionManager === this && lock.transactionActive) {
        return true;
      }
    }
    return false;
  }

  // The metrics tracker, or null when not inside a transaction.
  getMetricsTracker(): TransactionMetricsTracker | null {
    return this._metricsTracker;
  }

  getFilesRead(): Set<string> {
    if (this._filesRead === null) {
      throw new Error("Transaction not started");
    }
    return this._filesRead;
  }

  getTime(): number {
    if (this._udfTime === null) {
      throw new Error("Transaction not started");
    }
    return this._udfTime;
  }

  getSnapshotTs(): bigint {
    if (!this.isInTransaction()) {
      throw new Error(
        "getSnapshotTs() can only be called from a query or mutation.",
      );
    }
    // Top-level transactions are serialized and nested commits don't advance
    // _lastCommitTs, so it remains the latest commit known at transaction start.
    return this._lastCommitTs;
  }

  // Returns the transaction's commit timestamp, or null for a nested commit,
  // whose writes stay pending until the outermost transaction commits.
  commit(isNested: boolean): bigint | null {
    let commitTs: bigint | null = null;
    if (!isNested) {
      const nowNanos = BigInt(frameworkNow()) * 1_000_000n;
      // Assign a new timestamp only when committing the outermost transaction.
      // Bump by one if the clock is frozen or has moved backwards.
      commitTs =
        nowNanos > this._lastCommitTs ? nowNanos : this._lastCommitTs + 1n;
      this._lastCommitTs = commitTs;
    }
    const convex = getConvexGlobal();
    for (const component of Object.values(convex.components)) {
      component.db.commit(commitTs);
    }
    this._metricsTracker?.commit();
    this._endTransaction(isNested);
    return commitTs;
  }

  rollback(isNested: boolean) {
    const convex = getConvexGlobal();
    for (const component of Object.values(convex.components)) {
      component.db.rollbackWrites();
    }
    this._metricsTracker?.rollback();
    this._endTransaction(isNested);
  }

  _endTransaction(isNested: boolean) {
    if (this._markTransactionDone === null) {
      throw new Error("Transaction not started");
    }
    if (!isNested) {
      this._udfTime = null;
      this._metricsTracker = null;
      this._filesRead = null;
      this._waitOnCurrentFunction = null;
      this._markTransactionDone();
      this._markTransactionDone = null;
    }
  }

  stashWrites(): Map<string, Array<Record<DocumentId, StoredDocument | null>>> {
    const convex = getConvexGlobal();
    const stashed = new Map<
      string,
      Array<Record<DocumentId, StoredDocument | null>>
    >();
    for (const [path, component] of Object.entries(convex.components)) {
      stashed.set(path, component.db.stashWrites());
    }
    return stashed;
  }

  restoreWrites(
    stashed: Map<string, Array<Record<DocumentId, StoredDocument | null>>>,
  ) {
    const convex = getConvexGlobal();
    for (const [path, writes] of stashed) {
      convex.components[path].db.restoreWrites(writes);
    }
  }
}

/**
 * Call this function at the start of each of your tests.
 *
 * @param schema The default export from your "schema.ts" file.
 * @param modules If you have a custom `functions` path
 *     in convex.json, provide the function modules map by calling
 *     `import.meta.glob("./**\/*.*s")` from a file at the root of
 *     your Convex functions directory.
 * @returns an object which is by convention stored in the `t` variable
 *   and which provides methods for exercising your Convex functions.
 */
export function convexTest<Schema extends GenericSchema>(
  schema?: SchemaDefinition<Schema, boolean>,
  // For example `import.meta.glob("./**/*.*s")`
  modules?: Record<string, () => Promise<any>>,
): TestConvex<SchemaDefinition<Schema, boolean>>;
/**
 * Call this function at the start of each of your tests.
 *
 * @param options An optional configuration object with:
 *   - `schema`: The default export from your "schema.ts" file.
 *   - `modules`: If you have a custom `functions` path
 *     in convex.json, provide the function modules map by calling
 *     `import.meta.glob("./**\/*.*s")` from a file at the root of
 *     your Convex functions directory.
 *   - `transactionLimits`: Configure per-transaction bandwidth limits.
 *     - `false` (default): limits are not enforced
 *     - `true` or `{}`: enforce default Convex limits
 *     - `{ bytesRead: 1024 }`: override specific limits
 *   - `componentArgs`: Arguments for components' inits, keyed by component
 *     path (e.g. `"counter"` or `"counter/nested"`), in place of the ones in
 *     the parent's `convex.config`.
 *
 * Regardless of `transactionLimits`, a query may only call `.paginate()` once,
 * including nested query calls and component calls. Mutations may paginate
 * multiple times and call multiple queries that each paginate once.
 *
 * @returns an object which is by convention stored in the `t` variable
 *   and which provides methods for exercising your Convex functions.
 *
 * @example
 * const t = convexTest({ schema });
 * const t = convexTest({ schema, modules });
 */
export function convexTest<Schema extends GenericSchema>(options: {
  schema?: SchemaDefinition<Schema, boolean>;
  /** For example `import.meta.glob("./**\/*.*s")` */
  modules?: Record<string, () => Promise<any>>;
  transactionLimits?: Partial<TransactionMetrics> | boolean;
  componentArgs?: Record<string, Record<string, unknown>>;
}): TestConvex<SchemaDefinition<Schema, boolean>>;
export function convexTest<Schema extends GenericSchema>(
  schemaOrOptions?:
    | SchemaDefinition<Schema, boolean>
    | {
        schema?: SchemaDefinition<Schema, boolean>;
        modules?: Record<string, () => Promise<any>>;
        transactionLimits?: Partial<TransactionMetrics> | boolean;
        componentArgs?: Record<string, Record<string, unknown>>;
      },
  modules?: Record<string, () => Promise<any>>,
): TestConvex<SchemaDefinition<Schema, boolean>> {
  let schema: SchemaDefinition<Schema, boolean> | undefined = undefined;
  let limitsConfig: Partial<TransactionMetrics> | boolean = false;
  let componentArgs: Record<string, Record<string, unknown>> = {};

  // Detect legacy API by checking for schema's "tables" property
  function isSchemaDefinition(
    schemaOrOptions: unknown,
  ): schemaOrOptions is SchemaDefinition<Schema, boolean> {
    return (
      schemaOrOptions !== null &&
      typeof schemaOrOptions === "object" &&
      "tables" in schemaOrOptions &&
      typeof (schemaOrOptions as any).tables === "object"
    );
  }

  if (schemaOrOptions === undefined) {
    // convexTest() - no arguments
  } else if (isSchemaDefinition(schemaOrOptions)) {
    // convexTest(schema, modules?)
    schema = schemaOrOptions;
  } else {
    // convexTest({ schema?, modules?, transactionLimits? })
    const opts = schemaOrOptions;
    schema = opts.schema;
    modules = opts.modules;
    limitsConfig = opts.transactionLimits ?? false;
    componentArgs = opts.componentArgs ?? {};
  }

  return createConvexTest(
    schema,
    moduleCache(modules),
    limitsConfig,
    componentArgs,
  );
}

function createConvexTest<SchemaDef extends SchemaDefinition<any, boolean>>(
  schema: SchemaDef | undefined,
  modules: ComponentInfo["modules"],
  limitsConfig: Partial<TransactionMetrics> | boolean,
  componentArgs: Record<string, Record<string, unknown>> = {},
): TestConvex<SchemaDef> {
  const rootDb = new DatabaseFake(schema ?? null, ROOT_COMPONENT_PATH);
  const convexGlobal: ConvexGlobal = {
    components: {
      [ROOT_COMPONENT_PATH]: {
        db: rootDb,
        modules,
      },
    },
    transactionManager: new TransactionManager(limitsConfig),
    scheduler: new Scheduler(),
    syscall: syscallImpl(),
    asyncSyscall: asyncSyscallImpl(),
    jsSyscall: jsSyscallImpl(),
    deploymentJwtKey: null,
    componentArgs: { ...componentArgs },
    uninitializedComponents: new Set(),
  };
  guardEnvironmentVariablesInInits();
  ensureGlobalProxy();

  // Wrap all function properties to run within this test's ALS context.
  function wrapInContext<T extends Record<string, unknown>>(obj: T): T {
    const result = {} as any;
    for (const key of Object.keys(obj)) {
      const val = (obj as any)[key];
      result[key] =
        typeof val === "function"
          ? (...args: any[]) =>
              convexGlobalStorage.run(convexGlobal, async () => {
                await initializeRegisteredComponents();
                return await val(...args);
              })
          : val;
    }
    return result;
  }

  // Concurrent calls share one run of the pending inits. A failed run leaves
  // its components pending, like a push that was rolled back, so the next
  // call runs their inits again.
  let pendingInitRun: Promise<void> | null = null;
  function initializeRegisteredComponents(): Promise<void> {
    if (convexGlobal.uninitializedComponents.size === 0) {
      return Promise.resolve();
    }
    pendingInitRun ??= (async () => {
      const componentPaths = [...convexGlobal.uninitializedComponents];
      try {
        await runComponentInits(componentPaths);
        for (const componentPath of componentPaths) {
          convexGlobal.uninitializedComponents.delete(componentPath);
        }
      } finally {
        pendingInitRun = null;
      }
    })();
    return pendingInitRun;
  }

  function registerComponent<ComponentApi = unknown>(
    componentPath: string,
    schema: SchemaDefinition<GenericSchema, boolean>,
    glob: Record<string, () => Promise<any>>,
    options?: { args?: Record<string, unknown> },
  ) {
    const componentInfo = {
      db: new DatabaseFake(schema, componentPath),
      modules: moduleCache(glob),
    };
    convexGlobal.components[componentPath] = componentInfo;
    if (options?.args !== undefined) {
      convexGlobal.componentArgs[componentPath] = options.args;
    }
    convexGlobal.uninitializedComponents.add(componentPath);
    return componentsGeneric()[componentPath] as unknown as ComponentApi;
  }

  async function runInits() {
    await convexGlobalStorage.run(convexGlobal, async () => {
      await pendingInitRun?.catch(() => {});
      const componentPaths = Object.keys(convexGlobal.components).filter(
        (componentPath) => componentPath !== ROOT_COMPONENT_PATH,
      );
      await runComponentInits(componentPaths);
      convexGlobal.uninitializedComponents.clear();
    });
  }

  // Each accessor is immutable: `withIdentity` and `withRequestMetadata`
  // return a new one, keeping what the other method configured, so they can be
  // called in either order.
  function testAccessor(auth: AuthFake, requestOptions: RequestOptions): any {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars -- Not part of the test API.
    const { runInitMutations, ...functions } = withAuth(auth, requestOptions);
    return {
      ...wrapInContext(functions),
      jwks: () => convexGlobalStorage.run(convexGlobal, deploymentJwks),
      withIdentity(identity: Partial<UserIdentity>) {
        const subject =
          identity.subject ?? "" + simpleHash(JSON.stringify(identity));
        const issuer = identity.issuer ?? "https://convex.test";
        const tokenIdentifier =
          identity.tokenIdentifier ?? `${issuer}|${subject}`;
        const fullIdentity = { ...identity, subject, issuer, tokenIdentifier };
        return testAccessor(
          new AuthFake(fullIdentity, createTestJwt(fullIdentity)),
          requestOptions,
        );
      },
      withRequestMetadata(
        metadata: Partial<Pick<RequestMetadata, "ip" | "userAgent">>,
      ) {
        return testAccessor(auth, {
          ip: metadata.ip ?? null,
          userAgent: metadata.userAgent ?? null,
        });
      },
    };
  }

  return {
    ...testAccessor(new AuthFake(), DEFAULT_REQUEST_OPTIONS),
    registerComponent,
    runInits,
  };
}

// The backend's limits for a single init (`INIT_MAX_*` knobs). Inits can't
// read or write files.
const INIT_TRANSACTION_LIMITS: TransactionMetrics = {
  bytesRead: 1 << 14,
  bytesWritten: 1 << 14,
  databaseQueries: 100,
  documentsRead: 100,
  documentsWritten: 100,
  functionsScheduled: 10,
  scheduledFunctionArgsBytes: 1 << 14,
  filesWritten: 0,
  fileWriteBytes: 0,
  filesRead: 0,
  fileReadBytes: 0,
};

type ComponentInitCall = {
  componentPath: string;
  resolved: Awaited<ReturnType<typeof resolveFunction<"mutation">>>;
  args: Record<string, Value>;
};

/**
 * Run the inits of `componentPaths` as a push does: resolve each component's
 * arguments, then run every init in one transaction, each in its own nested
 * transaction. Every init runs even if an earlier one fails, and then the
 * whole transaction rolls back with an error listing each failure.
 */
async function runComponentInits(componentPaths: string[]) {
  const resolvedArgs = new Map<string, Promise<Record<string, Value>>>();
  const calls: ComponentInitCall[] = [];
  // Parents first, like the backend's walk of the component tree.
  const sorted = [...componentPaths].sort(
    (a, b) => a.split("/").length - b.split("/").length || a.localeCompare(b),
  );
  for (const componentPath of sorted) {
    const definition = await loadComponentDefinition(componentPath);
    const args = await componentInitArgs(componentPath, resolvedArgs);
    const initPath: string | undefined = definition?._init;
    if (initPath === undefined) {
      if (Object.keys(args).length > 0) {
        throw new Error(
          `Component "${componentPath}" is passed arguments, but it doesn't declare an init. ` +
            `Declare one with \`defineComponent(name, { init })\`.`,
        );
      }
      continue;
    }
    calls.push({
      componentPath,
      resolved: await resolveInit(componentPath, initPath),
      args,
    });
  }
  for (const { componentPath, resolved, args } of calls) {
    try {
      validateFunctionArgs(
        resolved.args,
        args,
        resolved.functionPath,
        /* allowPendingValues */ false,
      );
    } catch (e) {
      throw new Error(
        `Invalid arguments for the init of component "${componentPath}": ${(e as Error).message}`,
        { cause: e },
      );
    }
  }
  if (calls.length > 0) {
    await withAuth(new AuthFake(), DEFAULT_REQUEST_OPTIONS).runInitMutations(
      calls,
    );
  }
}

async function resolveInit(componentPath: string, initPath: string) {
  const functionPath = { componentPath, udfPath: initPath };
  let resolved;
  try {
    resolved = await resolveFunction(functionPath, "any");
  } catch {
    throw new Error(
      `Component "${componentPath}" declares ${initPath} as its init, but that function doesn't exist.`,
    );
  }
  if (!resolved.func.isMutation || !resolved.isInternal) {
    throw new Error(
      `Component "${componentPath}" declares ${initPath} as its init, but it isn't an ` +
        `internal mutation. Define it with \`internalMutation\`.`,
    );
  }
  return resolved;
}

function parentComponentPath(componentPath: string) {
  const index = componentPath.lastIndexOf("/");
  return index === -1 ? ROOT_COMPONENT_PATH : componentPath.slice(0, index);
}

/**
 * The default export of a component's (or the app's) `convex.config`, if its
 * modules include one: the object `defineComponent` or `defineApp` returned.
 */
async function loadComponentDefinition(componentPath: string): Promise<any> {
  const component = getConvexGlobal().components[componentPath];
  if (component === undefined || !component.modules.paths.includes(CONFIG)) {
    return undefined;
  }
  return (await component.modules(CONFIG)).default;
}

const CONFIG = "convex.config";

/**
 * The arguments a component's init receives: the ones passed in
 * `componentArgs` or `registerComponent`, or else the ones its parent's
 * `convex.config` passes to `use()`, resolved in the parent.
 */
function componentInitArgs(
  componentPath: string,
  memo: Map<string, Promise<Record<string, Value>>>,
): Promise<Record<string, Value>> {
  if (componentPath === ROOT_COMPONENT_PATH) {
    return Promise.resolve({});
  }
  let args = memo.get(componentPath);
  if (args === undefined) {
    args = (async () => {
      const parentPath = parentComponentPath(componentPath);
      const name = componentPath.slice(
        parentPath === ROOT_COMPONENT_PATH ? 0 : parentPath.length + 1,
      );
      const parentRegistered =
        getConvexGlobal().components[parentPath] !== undefined;
      const parentArgs = parentRegistered
        ? await componentInitArgs(parentPath, memo)
        : null;
      let rawArgs = getConvexGlobal().componentArgs[componentPath];
      if (rawArgs === undefined) {
        const parent = await loadComponentDefinition(parentPath);
        const instantiation = (
          parent?._childComponents as [string, unknown, unknown, unknown, any][]
        )?.find(([childName]) => childName === name);
        rawArgs = instantiation?.[4] ?? {};
      }
      const resolved: Record<string, Value> = {};
      for (const [argName, arg] of Object.entries(rawArgs)) {
        if (arg === undefined) {
          continue;
        }
        try {
          resolved[argName] = await resolveComponentArg(
            arg,
            parentPath,
            parentArgs,
          );
        } catch (e) {
          throw new Error(
            `Invalid argument ${argName} for component '${componentPath}': ${(e as Error).message}`,
            { cause: e },
          );
        }
      }
      return resolved;
    })();
    memo.set(componentPath, args);
  }
  return args;
}

/**
 * Resolve one argument passed in `use(component, { args })` like the backend
 * does: a function reference becomes a handle to the parent's function, a
 * child component (or a namespace of one) becomes an object of handles to its
 * public functions, and `component.args.<name>` becomes the parent's own
 * argument.
 */
async function resolveComponentArg(
  arg: unknown,
  parentPath: string,
  parentArgs: Record<string, Value> | null,
): Promise<Value> {
  if (typeof arg === "object" && arg !== null && !Array.isArray(arg)) {
    if (typeof (arg as any).__envVarRef === "string") {
      throw new Error(
        "A component argument can't be an environment variable. Pass it with the `env` option instead.",
      );
    }
    const argRef = (arg as any).__componentArgRef;
    if (typeof argRef === "string") {
      if (parentArgs === null || !(argRef in parentArgs)) {
        const parent =
          parentPath === ROOT_COMPONENT_PATH
            ? "The app"
            : `Component '${parentPath}'`;
        throw new Error(`${parent} has no argument named '${argRef}'.`);
      }
      return parentArgs[argRef];
    }
    const address = functionAddressOf(arg);
    if (address?.name !== undefined) {
      const functionPath = { componentPath: parentPath, udfPath: address.name };
      try {
        await resolveFunction(functionPath, "any");
      } catch {
        throw new Error(
          `${address.name} isn't a function in component "${parentPath}".`,
        );
      }
      return createFunctionHandle(functionPath);
    }
    if (address?.reference !== undefined) {
      return await resolveChildComponentReference(
        address.reference,
        parentPath,
      );
    }
    if (isSimpleObject(arg)) {
      const fields: Record<string, Value> = {};
      for (const [key, field] of Object.entries(arg)) {
        if (field !== undefined) {
          fields[key] = await resolveComponentArg(
            field,
            parentPath,
            parentArgs,
          );
        }
      }
      return fields;
    }
  }
  if (Array.isArray(arg)) {
    const elements: Value[] = [];
    for (const element of arg) {
      elements.push(await resolveComponentArg(element, parentPath, parentArgs));
    }
    return elements;
  }
  try {
    convexToJson(arg as Value);
  } catch (e) {
    throw new Error(`It isn't a valid argument: ${(e as Error).message}`, {
      cause: e,
    });
  }
  return arg as Value;
}

function functionAddressOf(
  value: object,
): { name?: string; reference?: string } | undefined {
  try {
    return getFunctionAddress(value);
  } catch {
    return undefined;
  }
}

// "_reference/childComponent/{name}/{attributes...}"
async function resolveChildComponentReference(
  reference: string,
  parentPath: string,
): Promise<Value> {
  const [, , name, ...attributes] = reference.split("/");
  const childPath =
    parentPath === ROOT_COMPONENT_PATH ? name : `${parentPath}/${name}`;
  if (getConvexGlobal().components[childPath] === undefined) {
    throw new Error(
      `The parent has no child component named '${name}'. Register it before running the inits.`,
    );
  }
  let exports: ExportTree | string = await componentExports(childPath);
  for (const attribute of attributes) {
    exports = typeof exports === "string" ? undefined! : exports[attribute];
    if (exports === undefined) {
      throw new Error(
        `Child component '${name}' doesn't export '${attributes.join(".")}'.`,
      );
    }
  }
  if (typeof exports !== "string" && Object.keys(exports).length === 0) {
    const location =
      attributes.length === 0 ? "" : ` under '${attributes.join(".")}'`;
    throw new Error(
      `Child component '${name}' doesn't export any functions${location}.`,
    );
  }
  return exportsToHandles(childPath, exports);
}

// Leaves are UDF paths.
type ExportTree = { [name: string]: ExportTree | string };

/**
 * A component's exports: its public functions, by module path and export
 * name, like the backend's file-based exports.
 */
async function componentExports(componentPath: string): Promise<ExportTree> {
  const modules = getModulesForComponent(componentPath);
  const tree: ExportTree = {};
  for (const modulePath of modules.paths) {
    if (
      modulePath === CONFIG ||
      modulePath === "schema" ||
      modulePath.startsWith("_generated/") ||
      /\.(test|spec)$/.test(modulePath)
    ) {
      continue;
    }
    const module = await modules(modulePath);
    for (const [exportName, func] of Object.entries(module ?? {})) {
      const f = func as any;
      if (!f?.isPublic || !(f.isQuery || f.isMutation || f.isAction)) {
        continue;
      }
      let node = tree;
      for (const segment of modulePath.split("/")) {
        const child = node[segment];
        if (typeof child === "string") {
          throw new Error(
            `${modulePath}:${exportName} conflicts with an existing export.`,
          );
        }
        node = node[segment] = child ?? {};
      }
      node[exportName] = `${modulePath}:${exportName}`;
    }
  }
  return tree;
}

function exportsToHandles(
  componentPath: string,
  exports: ExportTree | string,
): Value {
  if (typeof exports === "string") {
    return createFunctionHandle({ componentPath, udfPath: exports });
  }
  return Object.fromEntries(
    Object.entries(exports).map(([name, child]) => [
      name,
      exportsToHandles(componentPath, child),
    ]),
  );
}

function initFailedError(failures: string[]) {
  const whatFailed =
    failures.length === 1
      ? "an init failed"
      : `${failures.length} inits failed`;
  return new Error(
    `The push was rolled back because ${whatFailed}:\n\n${failures.join("\n\n")}`,
  );
}

/**
 * The runtime makes environment variables unavailable to inits, on every
 * read. Generated `env` exports hold the `process.env` object itself, so
 * guard that object rather than the `process` global, before function modules
 * load. Outside inits it behaves like the original object.
 */
let guardedEnv: object | null = null;
function guardEnvironmentVariablesInInits() {
  if (typeof process === "undefined" || process.env === guardedEnv) {
    return;
  }
  const check = () => {
    if (executionContextStorage.getStore()?.udfType === "init") {
      throw new Error("Can't use environment variables in an init function.");
    }
  };
  const env = process.env;
  guardedEnv = new Proxy(env, {
    get(target, property) {
      check();
      return Reflect.get(target, property);
    },
    has(target, property) {
      check();
      return Reflect.has(target, property);
    },
    ownKeys(target) {
      check();
      return Reflect.ownKeys(target);
    },
    getOwnPropertyDescriptor(target, property) {
      check();
      return Reflect.getOwnPropertyDescriptor(target, property);
    },
    set(target, property, value) {
      check();
      return Reflect.set(target, property, value);
    },
    deleteProperty(target, property) {
      check();
      return Reflect.deleteProperty(target, property);
    },
  });
  process.env = guardedEnv as NodeJS.ProcessEnv;
}

// Capture before fake timers are installed.
const realSetTimeout = globalThis.setTimeout.bind(globalThis);
const realClearTimeout = globalThis.clearTimeout.bind(globalThis);

function yieldThroughRealTimers(): Promise<void> {
  return new Promise<void>((r) => realSetTimeout(r, 0));
}

function withAuth(
  auth: AuthFake = authStorage.getStore() ?? new AuthFake(),
  requestOptions: RequestOptions = DEFAULT_REQUEST_OPTIONS,
) {
  // Auth doesn't propagate across component boundaries.
  function authForComponent(componentPath: string) {
    const ctx = executionContextStorage.getStore();
    const isCrossComponent =
      ctx !== undefined && componentPath !== ctx.componentPath;
    return isCrossComponent ? new AuthFake() : auth;
  }

  // Nested calls, including calls into components, run as part of the request
  // that triggered the outermost function, so they share its metadata.
  function requestMetadataForExecution(): RequestMetadataFake {
    return (
      requestMetadataStorage.getStore() ?? {
        ip: requestOptions.ip,
        userAgent: requestOptions.userAgent,
        requestId: newRequestId(),
        scheduledFunctionId: null,
        authToken: auth.token,
      }
    );
  }

  const runMutationWithHandler = async <T>(
    handler: (ctx: any, args: any) => T,
    args: any,
    extraCtx: any = {},
    functionPath: FunctionPath,
    isNested: boolean,
    transactionLimits?: Partial<TransactionMetrics>,
    isInternal = false,
    udfType: "mutation" | "init" = "mutation",
  ): Promise<T> => {
    // The wrapper decides what `ctx.meta.getFunctionMetadata()` reports as the
    // function's visibility, so match how the function was registered.
    const m = (isInternal ? internalMutationGeneric : mutationGeneric)({
      handler: (ctx: any, a: any) => {
        const testCtx = {
          ...ctx,
          auth: mutationAuth(authStorage.getStore() ?? auth),
          ...extraCtx,
        };
        return runWithGlobalOverrides(transactionGlobalOverrides(), () =>
          handler(testCtx, a),
        );
      },
    });
    const transactionManager = getTransactionManager();

    const authForChild = authForComponent(functionPath.componentPath);

    const parentCtx = executionContextStorage.getStore();
    const childCtx: ExecutionContext = {
      componentPath: functionPath.componentPath,
      udfPath: functionPath.udfPath,
      udfType,
      depth: (parentCtx?.depth ?? 0) + 1,
    };
    const requestMetadata = requestMetadataForExecution();

    await transactionManager.begin(isNested, transactionLimits);
    const childLock = new NestedLock(
      transactionManager,
      nestedTxStorage.getStore(),
    );
    try {
      const invokeRaw = () =>
        (
          m as unknown as {
            invokeMutation: (args: string) => Promise<string>;
          }
        ).invokeMutation(JSON.stringify(convexToJson([parseArgs(args)])));
      const invokeInContext = () =>
        executionContextStorage.run(childCtx, () =>
          requestMetadataStorage.run(requestMetadata, () =>
            authStorage.run(authForChild, invokeRaw),
          ),
        );

      const rawResult = await nestedTxStorage.run(childLock, invokeInContext);

      const commitTs = transactionManager.commit(isNested);
      const result = jsonToConvex(JSON.parse(rawResult));
      return commitTs === null
        ? (result as T)
        : (resolveCommittedValues(result, commitTs) as T);
    } catch (e) {
      transactionManager.rollback(isNested);
      throw deserializeConvexErrorData(e);
    } finally {
      childLock.transactionActive = false;
    }
  };

  // Shared helper to run a query with a given handler and transaction context.
  // Used by both queryFromPath (for function references) and inline queries.
  // Callers are responsible for acquiring the nested lock when nested.
  const runQueryWithHandler = async <T>(
    handler: (ctx: any, args: any) => T,
    args: any,
    functionPath: FunctionPath,
    isNested: boolean,
    transactionLimits?: Partial<TransactionMetrics>,
    isInternal = false,
  ): Promise<T> => {
    const q = (isInternal ? internalQueryGeneric : queryGeneric)({
      handler: (ctx: any, a: any) => {
        const testCtx = { ...ctx, auth: authStorage.getStore() ?? auth };
        return runWithGlobalOverrides(transactionGlobalOverrides(), () =>
          handler(testCtx, a),
        );
      },
    });
    const transactionManager = getTransactionManager();

    const authForChild = authForComponent(functionPath.componentPath);

    const parentCtx = executionContextStorage.getStore();
    const childCtx: ExecutionContext = {
      componentPath: functionPath.componentPath,
      udfPath: functionPath.udfPath,
      udfType: "query",
      depth: (parentCtx?.depth ?? 0) + 1,
      pagination: (isNested && parentCtx?.pagination) || { calls: 0 },
    };
    const requestMetadata = requestMetadataForExecution();

    await transactionManager.begin(isNested, transactionLimits);
    const childLock = new NestedLock(
      transactionManager,
      nestedTxStorage.getStore(),
    );
    try {
      const invokeRaw = () =>
        (
          q as unknown as { invokeQuery: (args: string) => Promise<string> }
        ).invokeQuery(JSON.stringify(convexToJson([parseArgs(args)])));
      const invokeInContext = () =>
        executionContextStorage.run(childCtx, () =>
          requestMetadataStorage.run(requestMetadata, () =>
            authStorage.run(authForChild, invokeRaw),
          ),
        );

      const rawResult = await nestedTxStorage.run(childLock, invokeInContext);

      const result = jsonToConvex(JSON.parse(rawResult));
      if (!isNested && hasPendingValues(result)) {
        throw new Error(
          `Function ${functionPath.udfPath} return value invalid: queries ` +
            `cannot return an unresolved placeholder value.`,
        );
      }
      return result as T;
    } catch (e) {
      throw deserializeConvexErrorData(e);
    } finally {
      childLock.transactionActive = false;
      transactionManager.rollback(isNested);
    }
  };

  // Shared helper to run an action with a given handler
  // Used by both actionFromPath (for function references) and inline actions
  const runActionWithHandler = async <T>(
    handler: (ctx: any, args: any) => T,
    args: any,
    functionPath: FunctionPath,
    ctxRunQuery: any,
    ctxRunMutation: any,
    ctxRunAction: any,
    isInternal = false,
  ): Promise<T> => {
    const a = (isInternal ? internalActionGeneric : actionGeneric)({
      handler: (ctx: any, innerArgs: any) => {
        const testCtx = {
          ...ctx,
          runQuery: ctxRunQuery,
          runMutation: ctxRunMutation,
          runAction: ctxRunAction,
          auth: authStorage.getStore() ?? auth,
        };
        return runWithGlobalOverrides({}, () => handler(testCtx, innerArgs));
      },
    });
    const authForChild = authForComponent(functionPath.componentPath);
    const parentCtx = executionContextStorage.getStore();
    const childCtx: ExecutionContext = {
      componentPath: functionPath.componentPath,
      udfPath: functionPath.udfPath,
      udfType: "action",
      depth: (parentCtx?.depth ?? 0) + 1,
    };
    const requestMetadata = requestMetadataForExecution();
    return await executionContextStorage.run(childCtx, async () => {
      try {
        const rawResult = await requestMetadataStorage.run(
          requestMetadata,
          () =>
            authStorage.run(authForChild, () =>
              (
                a as unknown as {
                  invokeAction: (
                    requestId: string,
                    args: string,
                  ) => Promise<string>;
                }
              ).invokeAction(
                requestMetadata.requestId,
                JSON.stringify(convexToJson([parseArgs(args)])),
              ),
            ),
        );
        return jsonToConvex(JSON.parse(rawResult)) as T;
      } catch (e) {
        throw deserializeConvexErrorData(e);
      }
    });
  };

  const byTypeWithPath = {
    queryFromPath: async (
      functionPath: FunctionPath,
      args: any,
      transactionLimits?: Partial<TransactionMetrics>,
    ) => {
      const parentLock = nestedTxStorage.getStore() ?? null;
      const isNested = parentLock !== null;
      if (parentLock) {
        await parentLock.acquire();
      }
      try {
        const resolved = await resolveFunction(functionPath, "query");
        validateFunctionArgs(
          resolved.args,
          args ?? {},
          resolved.functionPath,
          isNested,
        );

        const result = await runQueryWithHandler(
          resolved.handler,
          args,
          resolved.functionPath,
          isNested,
          transactionLimits,
          resolved.isInternal,
        );
        validateReturnValue(
          resolved.returns,
          result,
          "query",
          resolved.functionPath,
        );
        return result;
      } finally {
        if (parentLock) {
          parentLock.release();
        }
      }
    },

    snapshotQueryFromPath: async (
      functionPath: FunctionPath,
      args: any,
      transactionLimits?: Partial<TransactionMetrics>,
    ) => {
      const parentLock = nestedTxStorage.getStore() ?? null;
      if (parentLock) {
        await parentLock.acquire();
      }
      // Stash pending writes so the query only sees committed state.
      const transactionManager = getTransactionManager();
      const stashed = transactionManager.stashWrites();
      try {
        const resolved = await resolveFunction(functionPath, "query");
        validateFunctionArgs(
          resolved.args,
          args ?? {},
          resolved.functionPath,
          // Only reachable from inside a mutation.
          /* allowPendingValues */ true,
        );

        const result = await runQueryWithHandler(
          resolved.handler,
          args,
          resolved.functionPath,
          // isNested=true to avoid re-acquiring the top-level lock
          // (we're inside a mutation that already holds it).
          /* isNested */ true,
          transactionLimits,
          resolved.isInternal,
        );
        validateReturnValue(
          resolved.returns,
          result,
          "query",
          resolved.functionPath,
        );
        return result;
      } finally {
        transactionManager.restoreWrites(stashed);
        if (parentLock) {
          parentLock.release();
        }
      }
    },

    mutationFromPath: async (
      functionPath: FunctionPath,
      args: any,
      transactionLimits?: Partial<TransactionMetrics>,
    ): Promise<Value> => {
      const parentLock = nestedTxStorage.getStore() ?? null;
      const isNested = parentLock !== null;
      if (parentLock) {
        await parentLock.acquire();
      }
      try {
        const resolved = await resolveFunction(functionPath, "mutation");
        validateFunctionArgs(
          resolved.args,
          args ?? {},
          resolved.functionPath,
          isNested,
        );

        const result = await runMutationWithHandler(
          resolved.handler,
          args,
          {},
          resolved.functionPath,
          isNested,
          transactionLimits,
          resolved.isInternal,
        );
        validateReturnValue(
          resolved.returns,
          result,
          "mutation",
          resolved.functionPath,
        );
        return result;
      } finally {
        if (parentLock) {
          parentLock.release();
        }
      }
    },

    actionFromPath: async (functionPath: FunctionPath, args: any) => {
      const resolved = await resolveFunction(functionPath, "action");
      // An action runs outside any transaction.
      validateFunctionArgs(
        resolved.args,
        args ?? {},
        resolved.functionPath,
        /* allowPendingValues */ false,
      );

      const result = await runActionWithHandler(
        resolved.handler,
        args,
        resolved.functionPath,
        byTypeWithPath.runQuery,
        byTypeWithPath.runMutation,
        byTypeWithPath.runAction,
        resolved.isInternal,
      );
      validateReturnValue(
        resolved.returns,
        result,
        "action",
        resolved.functionPath,
      );
      return result;
    },
    runQuery: async (
      functionReference: FunctionReference<any, any, any, any>,
      args: any,
    ) => {
      const functionPath = getFunctionPathFromReference(functionReference);
      return await byTypeWithPath.queryFromPath(functionPath, args);
    },
    runMutation: async (
      functionReference: FunctionReference<any, any, any, any>,
      args: any,
    ) => {
      const functionPath = getFunctionPathFromReference(functionReference);
      return await byTypeWithPath.mutationFromPath(functionPath, args);
    },
    runAction: async (
      functionReference: FunctionReference<any, any, any, any>,
      args: any,
    ) => {
      const functionPath = getFunctionPathFromReference(functionReference);
      return await byTypeWithPath.actionFromPath(functionPath, args);
    },
  };

  const byType = {
    query: async (functionReferenceOrHandler: any, args?: any) => {
      if (typeof functionReferenceOrHandler === "function") {
        return await runQueryWithHandler(
          functionReferenceOrHandler,
          args ?? {},
          { componentPath: getCurrentComponentPath(), udfPath: "inline" },
          /* isNested */ false,
        );
      }
      const functionPath = getFunctionPathFromReference(
        functionReferenceOrHandler,
      );
      return await byTypeWithPath.queryFromPath(functionPath, args);
    },

    mutation: async (
      functionReferenceOrHandler: any,
      args?: any,
    ): Promise<Value> => {
      if (typeof functionReferenceOrHandler === "function") {
        return await runMutationWithHandler(
          functionReferenceOrHandler,
          args ?? {},
          /* extraCtx */ {},
          { componentPath: getCurrentComponentPath(), udfPath: "inline" },
          /* isNested */ false,
        );
      }
      const functionPath = getFunctionPathFromReference(
        functionReferenceOrHandler,
      );
      return await byTypeWithPath.mutationFromPath(functionPath, args);
    },

    action: async (functionReferenceOrHandler: any, args?: any) => {
      if (typeof functionReferenceOrHandler === "function") {
        return await runActionWithHandler(
          functionReferenceOrHandler,
          args ?? {},
          { componentPath: getCurrentComponentPath(), udfPath: "inline" },
          byTypeWithPath.runQuery,
          byTypeWithPath.runMutation,
          byTypeWithPath.runAction,
        );
      }
      const functionPath = getFunctionPathFromReference(
        functionReferenceOrHandler,
      );
      return await byTypeWithPath.actionFromPath(functionPath, args);
    },
  };
  const run = async <T>(
    componentPath: string,
    handler: (ctx: any) => T,
  ): Promise<T> => {
    // Grab StorageActionWriter from action ctx
    const a = actionGeneric({
      handler: async (ctx: GenericActionCtx<any>) => {
        return await runMutationWithHandler(
          handler,
          {},
          {
            storage: ctx.storage,
            // ActionCtx support was added in 0.0.40, removed in 0.0.41, should be added back in the future.
            // runAction: ctx.runAction.bind(ctx),
            // vectorSearch: ctx.vectorSearch.bind(ctx),
          },
          // Fake mutation path.
          {
            componentPath,
            udfPath: "custom",
          },
          false,
        );
      },
    });
    const requestMetadata = requestMetadataForExecution();
    try {
      const rawResult = await requestMetadataStorage.run(requestMetadata, () =>
        (
          a as unknown as {
            invokeAction: (requestId: string, args: string) => Promise<string>;
          }
        ).invokeAction(
          requestMetadata.requestId,
          JSON.stringify(convexToJson([{}])),
        ),
      );
      return jsonToConvex(JSON.parse(rawResult)) as T;
    } catch (e) {
      throw deserializeConvexErrorData(e);
    }
  };
  // The push's transaction: a top-level mutation that runs each init in a
  // nested transaction with the init limits.
  const runInitMutations = async (calls: ComponentInitCall[]) => {
    await runMutationWithHandler(
      async () => {
        const lock = nestedTxStorage.getStore()!;
        const failures: string[] = [];
        for (const { componentPath, resolved, args } of calls) {
          await lock.acquire();
          try {
            const result = await runMutationWithHandler(
              resolved.handler,
              args,
              {},
              resolved.functionPath,
              /* isNested */ true,
              INIT_TRANSACTION_LIMITS,
              /* isInternal */ true,
              "init",
            );
            validateReturnValue(
              resolved.returns,
              result,
              "mutation",
              resolved.functionPath,
            );
          } catch (e) {
            failures.push(
              `init ${resolved.functionPath.udfPath} of component "${componentPath}":\n${(e as Error).message}`,
            );
          } finally {
            lock.release();
          }
        }
        if (failures.length > 0) {
          throw initFailedError(failures);
        }
      },
      {},
      {},
      { componentPath: ROOT_COMPONENT_PATH, udfPath: "push" },
      /* isNested */ false,
    );
  };

  return {
    ...byType,
    ...byTypeWithPath,
    runInitMutations,

    runInComponent: run,
    run: async <T>(handler: (ctx: any) => T) => {
      return await run(getCurrentComponentPath(), handler);
    },

    fun: async (functionPath: FunctionPath, args: any) => {
      const { func } = await resolveFunction(functionPath, "any");
      if (func.isQuery) {
        return await byTypeWithPath.queryFromPath(functionPath, args);
      }
      if (func.isMutation) {
        return await byTypeWithPath.mutationFromPath(functionPath, args);
      }
      if (func.isAction) {
        return await byTypeWithPath.actionFromPath(functionPath, args);
      }
    },

    fetch: async (path: string, init?: RequestInit) => {
      const router: HttpRouter = (await getModules()("http"))["default"];
      if (!path.startsWith("/")) {
        throw new Error(`Path given to \`t.fetch\` must start with a \`/\``);
      }
      const url = new URL(`https://some.convex.site${path}`);
      const found = router.lookup(url.pathname, init?.method ?? ("GET" as any));
      if (!found) {
        return new Response(`No HttpAction routed for ${url.pathname}`, {
          status: 404,
        });
      }
      const [func] = found;
      const a = httpActionGeneric((ctx: any, a: any) => {
        const testCtx = {
          ...ctx,
          runQuery: byType.query,
          runMutation: byType.mutation,
          runAction: byType.action,
          auth,
        };
        return runWithGlobalOverrides({}, () => getHandler(func)(testCtx, a));
      });
      const httpCtx: ExecutionContext = {
        componentPath: getCurrentComponentPath(),
        udfPath: "http",
        udfType: "http",
        depth: 0,
      };
      // The request's headers are not inspected: like the identity, the IP and
      // the user agent are configured on the test accessor.
      const requestMetadata = requestMetadataForExecution();
      const response = await executionContextStorage.run(httpCtx, () =>
        requestMetadataStorage.run(requestMetadata, () =>
          (
            a as unknown as {
              invokeHttpAction: (request: Request) => Promise<Response>;
            }
          ).invokeHttpAction(new Request(url, init)),
        ),
      );
      return response;
    },

    finishInProgressScheduledFunctions: (): Promise<void> =>
      getConvexGlobal().scheduler.finishInProgressScheduledFunctions(),

    finishAllScheduledFunctions: async (
      advanceTimers: () => void,
      maxIterations: number = 100,
    ): Promise<void> => {
      // Wait for all scheduled functions to finish, advancing time in between
      // each function.
      // Stop after a fixed number of iterations to avoid infinite loops.
      const { scheduler } = getConvexGlobal();
      const originalSetTimeout = globalOverridesStorage.exit(
        () => globalThis.setTimeout,
      );
      const advanceTimersIfActive = () => {
        const currentSetTimeout = globalOverridesStorage.exit(
          () => globalThis.setTimeout,
        );
        if (currentSetTimeout !== originalSetTimeout) {
          throw new Error(
            "finishAllScheduledFunctions: timers were restored or replaced " +
              "while waiting for scheduled functions.",
          );
        }
        advanceTimers();
      };
      // A function scheduled while real timers were active sits on a real
      // setTimeout that advanceTimers (fake timers) cannot fire. If it has
      // already expired it fires within a few real event loop turns, so
      // yield and re-check before concluding there is nothing left to run.
      // If it still hasn't fired after this many consecutive idle turns,
      // its scheduled time hasn't arrived on the real clock and it can
      // never run during this test, so it is skipped, matching the
      // behavior before the in-memory scheduler existed.
      const maxIdleTurns = 20;
      let idleTurns = 0;
      for (let i = 0; i < maxIterations; i++) {
        advanceTimersIfActive();
        if (!scheduler.anyFunctionsRunning()) {
          if (!scheduler.anyPendingTimers() || idleTurns >= maxIdleTurns) {
            return;
          }
          idleTurns += 1;
          await yieldThroughRealTimers();
          continue;
        }
        idleTurns = 0;
        // Advance timers while waiting for scheduled functions to finish.
        // Stop if teardown restores or replaces the clock.
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
          await new Promise<void>((resolve, reject) => {
            const pump = () => {
              try {
                advanceTimersIfActive();
                timer = realSetTimeout(pump, 0);
              } catch (error) {
                // Preserve the value thrown by advanceTimers.
                // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors
                reject(error);
              }
            };
            void scheduler
              .finishInProgressScheduledFunctions()
              .then(resolve, reject);
            pump();
          });
        } finally {
          realClearTimeout(timer);
        }
      }
      throw new Error(
        "finishAllScheduledFunctions: too many iterations. " +
          "Check for infinitely recursive scheduled functions, " +
          "or increase maxIterations.",
      );
    },
  };
}

function parseArgs(
  args: Record<string, Value> | undefined,
): Record<string, Value> {
  if (args === undefined) {
    return {};
  }
  if (!isSimpleObject(args)) {
    throw new Error(
      `The arguments to a Convex function must be an object. Received: ${
        args as any
      }`,
    );
  }
  return args;
}

function validateFunctionArgs(
  argsValidator: ValidatorJSON,
  args: any,
  functionPath: FunctionPath,
  allowPendingValues: boolean,
) {
  if (!allowPendingValues && hasPendingValues(args)) {
    throw new Error(
      `Invalid arguments for "${functionPath.udfPath}": an unresolved ` +
        `placeholder value can only be passed to a query or mutation ` +
        `called from a mutation`,
    );
  }
  validateValidator(argsValidator, resolvePendingValues(args));
}

function validateReturnValue(
  returnsValidator: ValidatorJSON | null,
  result: any,
  functionType: "query" | "mutation" | "action",
  functionPath: FunctionPath,
) {
  if (returnsValidator !== null) {
    try {
      validateValidator(returnsValidator, resolvePendingValues(result));
    } catch (e) {
      throw new Error(
        `Return value validation failed for ${functionType} "${functionPath.udfPath}":\n${(e as Error).message}`,
        { cause: e },
      );
    }
  }
}

function createFunctionHandle(functionPath: FunctionPath) {
  return `function://${functionPath.componentPath};${functionPath.udfPath}`;
}

function parseFunctionHandle(handle: string) {
  const [componentPath, udfPath] = handle.split("function://")[1].split(";");
  return { componentPath, udfPath };
}

function getFunctionPathFromAddress(
  functionAddress:
    | { name: string; reference: undefined; functionHandle: undefined }
    | { reference: string; name: undefined; functionHandle: undefined }
    | { functionHandle: string; name: undefined; reference: undefined },
): FunctionPath {
  if (functionAddress.functionHandle !== undefined) {
    return parseFunctionHandle(functionAddress.functionHandle);
  }
  if (functionAddress.name !== undefined) {
    return {
      udfPath: functionAddress.name,
      componentPath: getCurrentComponentPath(),
    };
  }
  if (functionAddress.reference !== undefined) {
    // "_reference/childComponent/aggregate/path/to/file/functionName" -> "aggregate/path/to/file"
    const childComponentName = functionAddress.reference.split("/")[2];
    let componentPath = childComponentName;
    if (getCurrentComponentPath().length > 0) {
      componentPath = `${getCurrentComponentPath()}/${componentPath}`;
    }
    // "_reference/childComponent/aggregate/path/to/file/functionName" -> "path/to/file/functionName"
    const functionNameWithSlashes = functionAddress.reference
      .split("/")
      .slice(3)
      .join("/");
    // "path/to/file/functionName" -> "path/to/file:functionName"
    const filepath = functionNameWithSlashes.split("/").slice(0, -1).join("/");
    const functionName = functionNameWithSlashes.split("/").pop();
    return {
      udfPath: `${filepath}:${functionName}`,
      componentPath,
    };
  }
  throw new Error("Function address not supported");
}

function getFunctionPathFromReference(
  functionReference: FunctionReference<any, any, any, any>,
) {
  return getFunctionPathFromAddress(getFunctionAddress(functionReference));
}

type RegisteredFunctions = {
  query: RegisteredQuery<any, any, Value>;
  mutation: RegisteredMutation<any, any, Value>;
  action: RegisteredAction<any, any, any>;
  any: any;
};

type RegisteredFunctionKind = keyof RegisteredFunctions;

function getHandler<Args extends DefaultFunctionArgs, Returns>(
  func: RegisteredAction<any, Args, Returns>,
): (ctx: GenericActionCtx<any>, args: Args) => Promise<Returns>;
function getHandler<Args extends DefaultFunctionArgs, Returns>(
  func: RegisteredMutation<any, Args, Returns>,
): (ctx: GenericActionCtx<any>, args: Args) => Promise<Returns>;
function getHandler<Args extends DefaultFunctionArgs, Returns>(
  func: RegisteredQuery<any, Args, Returns>,
): (ctx: GenericActionCtx<any>, args: Args) => Promise<Returns>;
function getHandler(
  func: PublicHttpAction,
): (ctx: GenericActionCtx<any>, args: Request) => Promise<Response>;
function getHandler(func: any): (ctx: any, args: any) => any {
  return "_handler" in func ? func["_handler"] : func;
}

async function resolveFunction<T extends RegisteredFunctionKind>(
  functionPath: FunctionPath,
  type: T,
): Promise<{
  func: RegisteredFunctions[T];
  functionPath: FunctionPath;
  handler: (ctx: any, args: any) => any;
  returns: ValidatorJSON | null;
  args: ValidatorJSON;
  isInternal: boolean;
}> {
  // "queries/messages:list" -> ["queries/messages", "list"]
  const [modulePath, maybeExportName] = functionPath.udfPath.split(":");
  const exportName =
    maybeExportName === undefined ? "default" : maybeExportName;

  const module = await getModulesForComponent(functionPath.componentPath)(
    modulePath,
  );

  const func = module[exportName];
  if (func === undefined) {
    throw new Error(
      `Expected a Convex function exported from module "${modulePath}" as \`${exportName}\`, but there is no such export.`,
    );
  }
  const handler = getHandler(func);
  if (typeof handler !== "function") {
    throw new Error(
      `Expected a Convex function exported from module "${modulePath}" as \`${exportName}\`, but got: ${func}`,
    );
  }
  switch (type) {
    case "query":
      if (!func.isQuery) {
        throw new Error(
          `Expected a query function, but the function exported from module "${modulePath}" as \`${exportName}\` is not a query.`,
        );
      }
      break;
    case "mutation":
      if (!func.isMutation) {
        throw new Error(
          `Expected a mutation function, but the function exported from module "${modulePath}" as \`${exportName}\` is not a mutation.`,
        );
      }
      break;
    case "action":
      if (!func.isAction) {
        throw new Error(
          `Expected an action function, but the function exported from module "${modulePath}" as \`${exportName}\` is not an action.`,
        );
      }
      break;
    case "any":
      break;
    default:
      // eslint-disable-next-line @typescript-eslint/only-throw-error
      throw type satisfies never;
  }
  const args = JSON.parse(func.exportArgs()) as ValidatorJSON;
  // The real backend rejects these functions at push time, so surface the same
  // error as soon as the function is resolved.
  if (args.type !== "object" && args.type !== "any") {
    throw new Error(
      `Invalid JSON returned from ${modulePath}.js:${exportName}.exportArgs(): ` +
        `Args validator must be an object or any`,
    );
  }
  const returns = JSON.parse(func.exportReturns());

  const isInternal = (func as { isInternal?: boolean }).isInternal === true;

  return { func, functionPath, handler, returns, args, isInternal };
}

function simpleHash(string: string) {
  let hash = 0;
  for (let i = 0; i < string.length; i++) {
    const char = string.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  return hash;
}
