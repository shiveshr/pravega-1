package io.pravega.controller.store.stream;

import io.pravega.common.Exceptions;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.tables.Cache;
import io.pravega.controller.store.stream.tables.Data;
import io.pravega.shared.segment.StreamSegmentNameUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public abstract class AbstractStreamWithCache<T> extends PersistentStreamBase<T> {
    protected final Cache<T> cache;

    public AbstractStreamWithCache(String scopeName, String streamName, Supplier<Cache<T>> cacheSupplier) {
        super(scopeName, streamName);
        this.cache = cacheSupplier.get();
    }

    // region overrides
    private CompletableFuture<Long> getCreationTime() {
        return cache.getCachedData(getCreateTimeKey())
                .thenApply(data -> BitConverter.readLong(data.getData(), 0));
    }

    protected abstract String getCreateTimeKey();

    @Override
    CompletableFuture<Data<T>> getCurrentEpochRecordData(boolean ignoreCached) {
        String currentEpochDataKey = getCurrentEpochDataKey();
        if (ignoreCached) {
            cache.invalidateCache(currentEpochDataKey);
        }
        return cache.getCachedData(currentEpochDataKey);
    }

    protected abstract String getCurrentEpochDataKey();

    @Override
    CompletableFuture<Data<T>> getEpochRecordData(int epoch) {
        return cache.getCachedData(getEpochDataKey(epoch));
    }

    protected abstract String getEpochDataKey(int epoch);

    @Override
    CompletableFuture<Data<T>> getEpochIndexRootData(boolean ignoreCached) {
        String epochIndexRootKey = getEpochIndexRootKey();
        if (ignoreCached) {
            cache.invalidateCache(epochIndexRootKey);
        }

        return cache.getCachedData(epochIndexRootKey);
    }

    protected abstract String getEpochIndexRootKey();

    @Override
    CompletableFuture<Data<T>> getEpochIndexLeafData(boolean ignoreCached, int leaf) {
        String epochIndexLeafKey = getEpochIndexLeafKey(leaf);
        if (ignoreCached) {
            cache.invalidateCache(epochIndexLeafKey);
        }

        return cache.getCachedData(epochIndexLeafKey);
    }

    protected abstract String getEpochIndexLeafKey(int leaf);

    @Override
    CompletableFuture<Data<T>> getSealedSegmentEpochRecord(long segmentId) {
        // read sealed segment map shard
        return cache.getCachedData(getSealedSegmentRecordKey(segmentId));
    }

    protected abstract String getSealedSegmentRecordKey(long segmentId);

    @Override
    CompletableFuture<Data<T>> getSealedSegmentMapShard(long segmentId, boolean ignoreCached) {
        int epoch = StreamSegmentNameUtils.getEpoch(segmentId);
        String sealedSegmentShardKey = getSealedSegmentShardKey(epoch);
        if (ignoreCached) {
            cache.invalidateCache(sealedSegmentShardKey);
        }
        return cache.getCachedData(sealedSegmentShardKey);
    }

    protected abstract String getSealedSegmentShardKey(int epoch);

    @Override
    CompletableFuture<Data<T>> getMarkerData(long segmentId) {
        final CompletableFuture<Data<T>> result = new CompletableFuture<>();
        final String key = getMarkerKey(segmentId);
        cache.getCachedData(key)
                .whenComplete((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = Exceptions.unwrap(ex);
                        if (cause instanceof StoreException.DataNotFoundException) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(cause);
                        }
                    } else {
                        result.complete(res);
                    }
                });

        return result;
    }

    protected abstract String getMarkerKey(long segmentId);

    @Override
    CompletableFuture<Data<T>> getTruncationData(boolean ignoreCached) {
        String truncationKey = getTruncationKey();
        if (ignoreCached) {
            cache.invalidateCache(truncationKey);
        }

        return cache.getCachedData(truncationKey);
    }

    protected abstract String getTruncationKey();

    @Override
    CompletableFuture<Data<T>> getConfigurationData(boolean ignoreCached) {
        String configurationKey = getConfigurationKey();
        if (ignoreCached) {
            cache.invalidateCache(configurationKey);
        }

        return cache.getCachedData(configurationKey);
    }

    protected abstract String getConfigurationKey();

    @Override
    CompletableFuture<Data<T>> getStateData(boolean ignoreCached) {
        String stateKey = getStateKey();
        if (ignoreCached) {
            cache.invalidateCache(stateKey);
        }

        return cache.getCachedData(stateKey);
    }

    protected abstract String getStateKey();
    // endregion
}
