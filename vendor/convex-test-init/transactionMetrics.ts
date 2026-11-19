export type TransactionMetrics = {
  bytesRead: number;
  bytesWritten: number;
  databaseQueries: number;
  documentsRead: number;
  documentsWritten: number;
  functionsScheduled: number;
  scheduledFunctionArgsBytes: number;
  filesWritten: number;
  fileWriteBytes: number;
  filesRead: number;
  fileReadBytes: number;
};

// Metrics that track reads. On rollback these are still folded into the
// parent (the reads happened), unlike the write metrics whose effects are
// undone.
const READ_METRIC_KEYS: (keyof TransactionMetrics)[] = [
  "bytesRead",
  "documentsRead",
  "databaseQueries",
  "filesRead",
  "fileReadBytes",
];

const MiB = 1 << 20;

const DEFAULT_TRANSACTION_LIMITS: TransactionMetrics = {
  bytesRead: 16 * MiB,
  bytesWritten: 16 * MiB,
  documentsRead: 32_000,
  databaseQueries: 4_096,
  documentsWritten: 16_000,
  functionsScheduled: 1000,
  scheduledFunctionArgsBytes: 16 * MiB,
  filesWritten: 10,
  fileWriteBytes: 16 * MiB,
  filesRead: 10,
  fileReadBytes: 16 * MiB,
};

function zeroMetrics(): TransactionMetrics {
  return {
    bytesRead: 0,
    bytesWritten: 0,
    documentsRead: 0,
    documentsWritten: 0,
    databaseQueries: 0,
    functionsScheduled: 0,
    scheduledFunctionArgsBytes: 0,
    filesWritten: 0,
    fileWriteBytes: 0,
    filesRead: 0,
    fileReadBytes: 0,
  };
}

// One (possibly nested) transaction's accumulated usage, its limits, and
// whether those limits are enforced. Layers form a stack, mirroring the
// database's nested write layers.
type MetricsLayer = {
  metrics: TransactionMetrics;
  limits: TransactionMetrics;
  enforce: boolean;
};

/**
 * Tracks per-transaction bandwidth usage and enforces limits.
 *
 * Like `DatabaseFake`, which owns a stack of nested write layers and exposes
 * `startTransaction` / `commit` / `rollbackWrites`, this tracker owns a stack
 * of metrics layers and exposes the matching lifecycle. The
 * `TransactionManager` drives both in lockstep.
 *
 * Reads and writes are counted against the innermost (top) layer. A nested
 * `ctx.runQuery` / `ctx.runMutation` pushes a child layer that inherits the
 * parent's accumulated usage (so the global limit still applies to the whole
 * transaction) plus any tighter `transactionLimits`. On commit the child's
 * usage folds back into its parent; on rollback only the reads fold back,
 * since the writes were undone.
 */
export class TransactionMetricsTracker {
  private _config: Partial<TransactionMetrics> | boolean;
  private _layers: MetricsLayer[] = [];

  constructor(config: Partial<TransactionMetrics> | boolean = false) {
    this._config = config;
  }

  // Mirrors `DatabaseFake.startTransaction`. Pushes a layer for the new
  // transaction: the top-level layer uses the configured limits; a nested
  // layer inherits its parent's usage plus any tighter `transactionLimits`.
  startTransaction(transactionLimits?: Partial<TransactionMetrics>) {
    const parent = this._current();
    this._layers.push(
      parent === undefined
        ? this._rootLayer()
        : this._childLayer(parent, transactionLimits),
    );
  }

  // Mirrors `DatabaseFake.commit`. Pops the layer and folds all of its usage
  // into the parent. Folding into nothing at the top level just ends tracking.
  commit() {
    const child = this._pop();
    const parent = this._current();
    if (parent !== undefined) {
      parent.metrics = { ...child.metrics };
    }
  }

  // Mirrors `DatabaseFake.rollbackWrites`. Pops the layer, discarding its
  // writes. Reads still fold into the parent (they happened), but the writes
  // were undone so they don't count against the transaction.
  rollback() {
    const child = this._pop();
    const parent = this._current();
    if (parent !== undefined) {
      for (const key of READ_METRIC_KEYS) {
        parent.metrics[key] = child.metrics[key];
      }
    }
  }

  trackRead(docSizeBytes: number) {
    const layer = this._current();
    if (layer === undefined) {
      return;
    }
    layer.metrics.bytesRead += docSizeBytes;
    layer.metrics.documentsRead += 1;
    if (layer.enforce) {
      if (layer.metrics.bytesRead > layer.limits.bytesRead) {
        throw new Error(
          `Read too much data in a single function execution (limit: ${layer.limits.bytesRead} bytes). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
      if (layer.metrics.documentsRead > layer.limits.documentsRead) {
        throw new Error(
          `Scanned too many documents in a single function execution (limit: ${layer.limits.documentsRead}). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
    }
  }

  trackWrite(docSizeBytes: number) {
    const layer = this._current();
    if (layer === undefined) {
      return;
    }
    layer.metrics.bytesWritten += docSizeBytes;
    layer.metrics.documentsWritten += 1;
    if (layer.enforce) {
      if (layer.metrics.bytesWritten > layer.limits.bytesWritten) {
        throw new Error(
          `Wrote too much data in a single function execution (limit: ${layer.limits.bytesWritten} bytes). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
      if (layer.metrics.documentsWritten > layer.limits.documentsWritten) {
        throw new Error(
          `Wrote too many documents in a single function execution (limit: ${layer.limits.documentsWritten}). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
    }
  }

  trackIndexRange() {
    const layer = this._current();
    if (layer === undefined) {
      return;
    }
    layer.metrics.databaseQueries += 1;
    if (layer.enforce) {
      if (layer.metrics.databaseQueries > layer.limits.databaseQueries) {
        throw new Error(
          `Too many index ranges read in a single function execution (limit: ${layer.limits.databaseQueries}). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
    }
  }

  trackScheduledFunction(argsSizeBytes: number) {
    const layer = this._current();
    if (layer === undefined) {
      return;
    }
    layer.metrics.functionsScheduled += 1;
    layer.metrics.scheduledFunctionArgsBytes += argsSizeBytes;
    if (layer.enforce) {
      if (layer.metrics.functionsScheduled > layer.limits.functionsScheduled) {
        throw new Error(
          `Scheduled too many functions in a single function execution (limit: ${layer.limits.functionsScheduled}). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
      if (
        layer.metrics.scheduledFunctionArgsBytes >
        layer.limits.scheduledFunctionArgsBytes
      ) {
        throw new Error(
          `Scheduled function arguments too large in a single function execution (limit: ${layer.limits.scheduledFunctionArgsBytes} bytes). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
    }
  }

  // Unlike the other limits, the file limits are checked before counting the
  // new file, matching the backend, so a caught error leaves usage unchanged.
  trackFileWrite(sizeBytes: number) {
    const layer = this._current();
    if (layer === undefined) {
      return;
    }
    if (layer.enforce) {
      if (layer.metrics.filesWritten >= layer.limits.filesWritten) {
        throw new Error(
          `Too many files stored by this mutation (limit: ${layer.limits.filesWritten}). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
      if (
        layer.metrics.fileWriteBytes + sizeBytes >
        layer.limits.fileWriteBytes
      ) {
        throw new Error(
          `Too large total size of the files stored by this mutation (limit: ${layer.limits.fileWriteBytes} bytes). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
    }
    layer.metrics.filesWritten += 1;
    layer.metrics.fileWriteBytes += sizeBytes;
  }

  // Deleting a file this transaction stored gives back its write budget.
  untrackFileWrite(sizeBytes: number) {
    const layer = this._current();
    if (layer === undefined) {
      return;
    }
    layer.metrics.filesWritten -= 1;
    layer.metrics.fileWriteBytes -= sizeBytes;
  }

  trackFileRead(sizeBytes: number) {
    const layer = this._current();
    if (layer === undefined) {
      return;
    }
    if (layer.enforce) {
      if (layer.metrics.filesRead >= layer.limits.filesRead) {
        throw new Error(
          `Too many files read by this mutation (limit: ${layer.limits.filesRead}). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
      if (
        layer.metrics.fileReadBytes + sizeBytes >
        layer.limits.fileReadBytes
      ) {
        throw new Error(
          `Too large total size of the files read by this mutation (limit: ${layer.limits.fileReadBytes} bytes). ` +
            `This is a Convex limit: https://docs.convex.dev/production/state/limits`,
        );
      }
    }
    layer.metrics.filesRead += 1;
    layer.metrics.fileReadBytes += sizeBytes;
  }

  getTransactionMetrics() {
    const layer = this._current();
    if (layer === undefined) {
      throw new Error("Transaction not started");
    }
    const { metrics, limits } = layer;
    return {
      bytesRead: {
        used: metrics.bytesRead,
        remaining: limits.bytesRead - metrics.bytesRead,
      },
      bytesWritten: {
        used: metrics.bytesWritten,
        remaining: limits.bytesWritten - metrics.bytesWritten,
      },
      databaseQueries: {
        used: metrics.databaseQueries,
        remaining: limits.databaseQueries - metrics.databaseQueries,
      },
      documentsRead: {
        used: metrics.documentsRead,
        remaining: limits.documentsRead - metrics.documentsRead,
      },
      documentsWritten: {
        used: metrics.documentsWritten,
        remaining: limits.documentsWritten - metrics.documentsWritten,
      },
      functionsScheduled: {
        used: metrics.functionsScheduled,
        remaining: limits.functionsScheduled - metrics.functionsScheduled,
      },
      scheduledFunctionArgsBytes: {
        used: metrics.scheduledFunctionArgsBytes,
        remaining:
          limits.scheduledFunctionArgsBytes -
          metrics.scheduledFunctionArgsBytes,
      },
      filesWritten: {
        used: metrics.filesWritten,
        remaining: limits.filesWritten - metrics.filesWritten,
      },
      fileWriteBytes: {
        used: metrics.fileWriteBytes,
        remaining: limits.fileWriteBytes - metrics.fileWriteBytes,
      },
      filesRead: {
        used: metrics.filesRead,
        remaining: limits.filesRead - metrics.filesRead,
      },
      fileReadBytes: {
        used: metrics.fileReadBytes,
        remaining: limits.fileReadBytes - metrics.fileReadBytes,
      },
    };
  }

  private _current(): MetricsLayer | undefined {
    return this._layers[this._layers.length - 1];
  }

  private _pop(): MetricsLayer {
    const layer = this._layers.pop();
    if (layer === undefined) {
      throw new Error("Transaction not started");
    }
    return layer;
  }

  private _rootLayer(): MetricsLayer {
    if (this._config === false) {
      return {
        metrics: zeroMetrics(),
        limits: { ...DEFAULT_TRANSACTION_LIMITS },
        enforce: false,
      };
    }
    if (this._config === true) {
      return {
        metrics: zeroMetrics(),
        limits: { ...DEFAULT_TRANSACTION_LIMITS },
        enforce: true,
      };
    }
    return {
      metrics: zeroMetrics(),
      limits: { ...DEFAULT_TRANSACTION_LIMITS, ...this._config },
      enforce: true,
    };
  }

  private _childLayer(
    parent: MetricsLayer,
    transactionLimits?: Partial<TransactionMetrics>,
  ): MetricsLayer {
    const limits = { ...parent.limits };
    if (transactionLimits !== undefined) {
      for (const key of Object.keys(limits) as (keyof TransactionMetrics)[]) {
        const requested = transactionLimits[key];
        if (requested !== undefined) {
          // Cap at the parent limit so a nested call can only lower the budget,
          // never raise it; offset by the parent's usage so the cap applies to
          // the nested call's own consumption.
          limits[key] = Math.min(
            parent.limits[key],
            parent.metrics[key] + requested,
          );
        }
      }
    }
    return {
      // Start from the parent's accumulated usage so the global limit applies
      // to the whole transaction, not just this nested call.
      metrics: { ...parent.metrics },
      limits,
      enforce: parent.enforce || transactionLimits !== undefined,
    };
  }
}
