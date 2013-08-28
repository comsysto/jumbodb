package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyChunksUtil;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Carsten Hufe
 */
@Deprecated // replace by ehcache
public class PseudoCacheForSnappy {
    public static final ConcurrentHashMap<File, SnappyChunks> SNAPPY_CHUNKS_CACHE = new ConcurrentHashMap<File, SnappyChunks>();
    public static final ConcurrentHashMap<ChunkRangeKey, BlockRange<?>> SNAPPY_CHUNK_RANGE_CACHE = new ConcurrentHashMap<ChunkRangeKey, BlockRange<?>>();

    public static SnappyChunks getSnappyChunksByFile(File compressedFile) {
        SnappyChunks snappyChunks = SNAPPY_CHUNKS_CACHE.get(compressedFile);
        if(snappyChunks == null) {
            snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(compressedFile);
            SNAPPY_CHUNKS_CACHE.put(compressedFile, snappyChunks);
        }
        return snappyChunks;
    }

    public static BlockRange<?> getSnappyChunkRange(File indexFile, long chunkIndex) {
        return SNAPPY_CHUNK_RANGE_CACHE.get(new ChunkRangeKey(indexFile, chunkIndex));
    }

    public static BlockRange<?> putSnappyChunkRange(File indexFile, long chunkIndex, BlockRange<?> blockRange) {
        return SNAPPY_CHUNK_RANGE_CACHE.put(new ChunkRangeKey(indexFile, chunkIndex), blockRange);
    }

    public static void clearCache() {
        SNAPPY_CHUNKS_CACHE.clear();
        SNAPPY_CHUNK_RANGE_CACHE.clear();
    }
}