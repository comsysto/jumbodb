package org.jumbodb.data.common.snappy;

import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyChunksUtil;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Carsten Hufe
 */
public class SnappyChunksWithCache {
    public static final ConcurrentHashMap<File, SnappyChunks> CACHE = new ConcurrentHashMap<File, SnappyChunks>();

    public static SnappyChunks getSnappyChunksByFile(File compressedFile) {
        SnappyChunks snappyChunks = CACHE.get(compressedFile);
        if(snappyChunks == null) {
            snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(compressedFile);
            CACHE.put(compressedFile, snappyChunks);
        }
        return snappyChunks;
    }

    public static void clearCache() {
        CACHE.clear();
    }
}