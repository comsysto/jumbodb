package org.jumbodb.database.service.query.data.snappy;

import net.minidev.json.parser.JSONParser;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyChunksUtil;
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
public abstract class AbstractJsonSnappyRetrieveDataSetsTask extends DefaultRetrieveDataSetsTask {
    protected final Cache dataSnappyChunksCache;
    protected JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);

    public AbstractJsonSnappyRetrieveDataSetsTask(Cache datasetsByOffsetsCache, ResultCallback resultCallback, DataStrategy strategy, String dateFormat, Set<FileOffset> offsets, JumboQuery searchQuery, File file, boolean scannedSearch, Cache dataSnappyChunksCache) {
        super(datasetsByOffsetsCache, resultCallback, strategy, dateFormat, offsets, searchQuery, file, scannedSearch);
        this.dataSnappyChunksCache = dataSnappyChunksCache;
    }

    protected SnappyChunks getSnappyChunksByFile() {
        Cache.ValueWrapper valueWrapper = dataSnappyChunksCache.get(file);
        if (valueWrapper != null) {
            return (SnappyChunks) valueWrapper.get();
        }
        SnappyChunks snappyChunksByFile = SnappyChunksUtil.getSnappyChunksByFile(file);
        dataSnappyChunksCache.put(file, snappyChunksByFile);
        return snappyChunksByFile;
    }

    protected long calculateChunkOffsetUncompressed(long chunkIndex, int snappyChunkSize) {
        return chunkIndex * snappyChunkSize;
    }

    protected long calculateChunkOffsetCompressed(long chunkIndex, List<Integer> snappyChunks) {
        long result = 0l;
        for (int i = 0; i < chunkIndex; i++) {
            result += snappyChunks.get(i) + 4; // 4 byte for length of chunk
        }
        return result + 16;
    }
}
