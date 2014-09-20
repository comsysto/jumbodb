package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.data.common.compression.CompressionBlocksUtil;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.common.DefaultRetrieveDataSetsTask;
import org.springframework.cache.Cache;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * Created by Carsten on 19.09.2014.
 */
public abstract class AbstractSnappyRetrieveDataSetsTask extends DefaultRetrieveDataSetsTask {
    protected final Cache dataSnappyChunksCache;
//    protected JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);

    public AbstractSnappyRetrieveDataSetsTask(Cache datasetsByOffsetsCache, ResultCallback resultCallback, DataStrategy strategy, String dateFormat, Set<FileOffset> offsets, JumboQuery searchQuery, File file, boolean scannedSearch, Cache dataSnappyChunksCache) {
        super(datasetsByOffsetsCache, resultCallback, strategy, dateFormat, offsets, searchQuery, file, scannedSearch);
        this.dataSnappyChunksCache = dataSnappyChunksCache;
    }

    protected Blocks getSnappyChunksByFile() {
        Cache.ValueWrapper valueWrapper = dataSnappyChunksCache.get(file);
        if (valueWrapper != null) {
            return (Blocks) valueWrapper.get();
        }
        Blocks blocksByFile = CompressionBlocksUtil.getBlocksByFile(file);
        dataSnappyChunksCache.put(file, blocksByFile);
        return blocksByFile;
    }

    protected long calculateBlockOffsetUncompressed(long chunkIndex, int compressionBlockSize) {
        return chunkIndex * compressionBlockSize;
    }

    protected long calculateBlockOffsetCompressed(long blockIndex, List<Integer> compressionBlocks) {
        long result = 0l;
        for (int i = 0; i < blockIndex; i++) {
            result += compressionBlocks.get(i) + 4; // 4 byte for length of chunk
        }
        return result + 16;
    }
}
