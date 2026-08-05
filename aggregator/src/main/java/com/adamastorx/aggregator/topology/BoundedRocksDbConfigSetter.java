package com.adamastorx.aggregator.topology;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/**
 * Bounds each RocksDB state-store instance's own memory footprint
 * explicitly, rather than leaving Kafka Streams' out-of-the-box RocksDB
 * defaults in place. This project's first Kafka Streams app, so there is
 * no in-repo sizing precedent to copy (backlog #81's own stated
 * resource-sizing requirement) -- reasoned here from first principles and
 * real numbers, not copied:
 *
 * <p><b>Why this exists at all.</b> With no {@code
 * rocksdb.config.setter}, each RocksDB instance uses librocksdb's own
 * native defaults: a 64MB {@code write_buffer_size} times {@code
 * max_write_buffer_number=2} (up to ~128MB of memtables) plus its own
 * unshared block cache, per store. This topology opens four persistent
 * stores ({@code price-window-store}, {@code sentiment-window-store}, and
 * -- added for the "latest known state" fallback, see this service's
 * README -- {@code latest-price-store}, {@code latest-sentiment-store});
 * left at RocksDB's own defaults that is several hundred MB of *reserved*
 * memory before a single byte of this milestone's actual data (a handful
 * of ticks/scores per ticker per 15-minute window, 5-8 tickers; the two
 * latest-known stores hold at most one record per ticker each, trivially
 * smaller still) is written -- a well-documented "Kafka Streams RocksDB
 * memory surprises people" failure mode, not a hypothetical one, and a
 * bad fit for a node with well under 1 free CPU core and no measured
 * memory slack yet (see this service's own platform deployment.yaml for
 * the real current node headroom numbers). This config setter applies to
 * every RocksDB-backed store the app opens (a single {@code
 * rocksdb.config.setter} in {@code application.yml}, not a per-store
 * hook) -- the two newer stores are bounded by the exact same config
 * below with no separate wiring needed.
 *
 * <p><b>What this sets instead.</b> A single 8MB block cache shared
 * across every store instance (RocksDB's own historical out-of-box
 * default for a single instance, deliberately kept small and shared
 * rather than per-store) via {@link LRUCache}, plus a 4MB write buffer
 * with at most 2 in memory at once (8MB memtable ceiling per store,
 * matching the point compaction-heavy small-write workloads use in
 * RocksDB's own tuning guide, not an arbitrary number) -- real, small,
 * explicit ceilings for a workload this milestone's own traffic
 * (5-8 tickers, single-digit events per ticker per window) will not come
 * close to saturating. Four stores under this config: roughly 8MB (shared
 * cache) + 4 x 8MB (per-store memtable ceiling) = ~40MB worst case, not
 * the several-hundred-MB unbounded ceiling RocksDB's own defaults leave
 * open. This is a bound, not a measurement -- real RSS has not been
 * sampled against a live deployment (out of scope here, see this
 * service's README/PR description for why); it is the deliberate,
 * documented ceiling this config enforces, not a guess presented as a
 * measured number.
 */
public class BoundedRocksDbConfigSetter implements RocksDBConfigSetter {

    // Shared once across every store this process opens (a lazily-created
    // singleton, not one per invocation) -- a per-store cache would defeat
    // the whole point of bounding total memory: N stores would each pay
    // for their own 8MB cache instead of sharing one.
    //
    // **Deliberately not a static final field initialized at class-load
    // time** -- an earlier draft was exactly that, and it broke real app
    // startup with a real, live UnsatisfiedLinkError: Kafka's own
    // StreamsConfig constructor resolves "rocksdb.config.setter" by
    // Class.forName-ing this class *purely to validate the config value*
    // (org.apache.kafka.common.config.ConfigDef#parseType), which runs
    // this class's static initializer -- including LRUCache's native
    // constructor -- before RocksDBStore's own code has had any chance to
    // call RocksDB.loadLibrary(), so the native library isn't linked yet.
    // Found live by actually running the built jar with
    // KAFKA_BOOTSTRAP_SERVERS pointed at a closed port (the same
    // unreachable-broker shape CI's own smoke-test step uses) -- the app
    // never got past context refresh. Lazy initialization plus an
    // explicit, idempotent RocksDB.loadLibrary() call in setConfig()
    // below (called by RocksDBStore itself, well after Kafka Streams'
    // config-validation phase) fixes this for real.
    private static volatile Cache sharedBlockCache;

    private static Cache sharedBlockCache() {
        Cache cache = sharedBlockCache;
        if (cache == null) {
            synchronized (BoundedRocksDbConfigSetter.class) {
                cache = sharedBlockCache;
                if (cache == null) {
                    RocksDB.loadLibrary();
                    cache = new LRUCache(8 * 1024 * 1024L);
                    sharedBlockCache = cache;
                }
            }
        }
        return cache;
    }

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        // Real, second live incident (backlog #85, found on this exact
        // fix's own first redeploy): a fresh `new BlockBasedTableConfig()`
        // here, rather than the instance Kafka Streams itself already
        // attached to `options`, broke RocksDBStore's own internal
        // metrics-recorder wiring the moment a store was actually opened
        // against real traffic -- a real
        // org.apache.kafka.streams.errors.ProcessorStateException ("The
        // used block-based table format configuration does not expose the
        // block cache ... Do not provide a new instance of
        // BlockBasedTableConfig") that took the whole stream client down
        // (SHUTDOWN_CLIENT), the same silent-but-Healthy-pod failure mode
        // the first RocksDB/Alpine bug had. Kafka Streams pre-configures
        // `options` with its own BlockBasedTableConfig before this method
        // runs specifically so its metrics recorder can introspect it
        // later -- `options.tableFormatConfig()` retrieves that exact
        // instance; mutating and reassigning it (not constructing a new
        // one) is what the exception message itself names as the fix.
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        tableConfig.setBlockCache(sharedBlockCache());
        tableConfig.setBlockSize(4 * 1024L);
        options.setTableFormatConfig(tableConfig);

        options.setWriteBufferSize(4 * 1024 * 1024L);
        options.setMaxWriteBufferNumber(2);

        // This milestone's real traffic (a handful of records per ticker
        // per window) never justifies more than one background
        // compaction/flush thread; RocksDB's own multi-thread defaults
        // are sized for write-heavy workloads this app doesn't have.
        options.setMaxBackgroundJobs(1);
    }

    @Override
    public void close(String storeName, Options options) {
        // Deliberately does not close SHARED_BLOCK_CACHE here -- it is
        // shared across every store this process opens, and RocksDBStore
        // calls close() per store, not once at process shutdown; closing
        // the shared cache on the first store's close would break every
        // other still-open store referencing it.
    }
}
