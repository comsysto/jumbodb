package org.jumbodb.database.service.query.data.common;

import org.apache.commons.lang.StringUtils;
import org.jumbodb.common.query.*;
import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.data.common.compression.CompressionBlocksUtil;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.snappy.CacheFileOffset;
import org.jumbodb.database.service.query.data.snappy.JsonSnappyLineBreakRetrieveDataSetsTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by Carsten on 19.09.2014.
 */
public abstract class DefaultRetrieveDataSetsTask implements Callable<Integer> {
    public static final byte[] EMPTY_BUFFER = new byte[0];
    protected final SimpleDateFormat simpleDateFormat;
    protected final boolean resultCacheEnabled;
    protected final File file;
    protected final Cache datasetsByOffsetsCache;
    protected final boolean scannedSearch;
    protected final long fileLength;
    protected final JumboQuery searchQuery;
    protected final ResultCallback resultCallback;
    protected final List<FileOffset> offsets;
    protected final Cache dataCompressionBlocksCache;
    protected Logger log = LoggerFactory.getLogger(JsonSnappyLineBreakRetrieveDataSetsTask.class);
    protected int results = 0;
    protected DataStrategy strategy;



    public DefaultRetrieveDataSetsTask(Cache datasetsByOffsetsCache, Cache dataCompressionBlocksCache,  ResultCallback resultCallback, DataStrategy strategy, String dateFormat, Set<FileOffset> offsets, JumboQuery searchQuery, File file, boolean scannedSearch) {
        this.datasetsByOffsetsCache = datasetsByOffsetsCache;
        this.dataCompressionBlocksCache = dataCompressionBlocksCache;
        this.resultCallback = resultCallback;
        this.resultCacheEnabled = searchQuery.isResultCacheEnabled();
        this.simpleDateFormat = new SimpleDateFormat(dateFormat, Locale.US);
        this.offsets = new LinkedList<FileOffset>(offsets);
        this.fileLength = file.length();
        this.searchQuery = searchQuery;
        this.file = file;
        this.scannedSearch = scannedSearch;
        this.strategy = strategy;
    }

    @Override
    public Integer call() throws Exception {
        if (scannedSearch) {
            fullScanData();
        } else {
            long start = System.currentTimeMillis();
            if (resultCacheEnabled) {
                List<FileOffset> leftOffsets = extractOffsetsFromCacheAndWriteResultForExisting();
                if (leftOffsets.isEmpty()) {
                    log.trace("Time for retrieving " + results + " only from cache " + file.getName() + " in " + (System
                            .currentTimeMillis() - start) + "ms");
                    return results;
                }
                int cacheResult = results;
                findLeftDatasetsAndWriteResults(leftOffsets);
                log.trace("Time for retrieving " + results + " (" + cacheResult + " from cache) datasets from " + file
                        .getName() + " in " + (System.currentTimeMillis() - start) + "ms");

            } else {
                findLeftDatasetsAndWriteResults(offsets);
                log.trace("Time for retrieving " + results + " (caching disabled) datasets from " + file
                        .getName() + " in " + (System.currentTimeMillis() - start) + "ms");
            }
        }
        return results;
    }

    protected abstract void findLeftDatasetsAndWriteResults(List<FileOffset> leftOffsets);

    protected abstract void fullScanData();

    protected List<FileOffset> extractOffsetsFromCacheAndWriteResultForExisting() throws IOException {
        List<FileOffset> notFoundOffsets = new ArrayList<FileOffset>(offsets.size());
        for (FileOffset offset : offsets) {
            Cache.ValueWrapper valueWrapper = datasetsByOffsetsCache.get(new CacheFileOffset(file, offset.getOffset()));
            if (valueWrapper != null) {
                Map<String, Object> dataSetFromOffsetsGroup = (Map<String, Object>) valueWrapper.get();
                IndexQuery indexQuery = offset.getIndexQuery();
                if (matchingFilter(dataSetFromOffsetsGroup, indexQuery.getAndJson())) {
                    if (!resultCallback.needsMore(searchQuery)) {
                        return Collections.emptyList(); // return empty list enough found!
                    }
                    resultCallback.writeResult(dataSetFromOffsetsGroup);
                    results++;
                }
            } else {
                notFoundOffsets.add(offset);
            }
        }
        return notFoundOffsets;
    }

    // in some cases you have to call skip in a buffered stream multiple times!
    protected long skipToOffset(BufferedInputStream bis, long chunkOffsetToSkip) throws IOException {
        long skipped = 0l;
        long lastSkipped = -1l;
        while (skipped != chunkOffsetToSkip || lastSkipped != 0l) {
            lastSkipped = bis.skip(chunkOffsetToSkip - skipped);
            skipped += lastSkipped;
        }
        return skipped;
    }

    protected byte[] getResultBuffer(byte[] lastBuffer, long toSkip) {
        if (toSkip >= 0) {
            return EMPTY_BUFFER;
        }
        int length = Math.abs((int) toSkip);
        byte[] last = new byte[length];
        System.arraycopy(lastBuffer, lastBuffer.length - length, last, 0, length);
        return last;
    }

    protected byte[] concat(int datasetStartOffset, byte[] readBuffer, byte[] resultBuffer, int readBufferLength) {
        int resBufLen = resultBuffer.length - datasetStartOffset;
        byte[] tmp = new byte[resBufLen + readBufferLength];
        if (resBufLen < 0) {
            int abs = Math.abs(resBufLen);
            System.arraycopy(readBuffer, abs, tmp, 0, readBufferLength - abs);
        } else {
            System.arraycopy(resultBuffer, datasetStartOffset, tmp, 0, resBufLen);
            System.arraycopy(readBuffer, 0, tmp, resBufLen, readBufferLength);
        }
        return tmp;
    }

    protected boolean matchingFilter(Map<String, Object> parsedJson, DataQuery jsonQuery) throws IOException {
        if (jsonQuery == null) {
            return true;
        }
        return matchingFilter(parsedJson, Arrays.asList(jsonQuery));
    }

    protected boolean matchingFilter(Map<String, Object> parsedJson, List<DataQuery> jsonQueries) {
        return matchingFilter(parsedJson, new HashMap<String, Object>(), jsonQueries);
    }

    // CARSTEN unit test
    protected boolean matchingFilter(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache,
                                     List<DataQuery> jsonQueries) {
        if (jsonQueries.size() == 0) {
            return true;
        }
        for (DataQuery jsonQuery : jsonQueries) {
            Object leftValue = findLeftValue(parsedJson, queriedValuesCache, jsonQuery);
            Object rightValue = findRightValue(parsedJson, queriedValuesCache, jsonQuery);


            // CARSTEN handle EXISTS QueryOperation at this position
            if (strategy.matches(jsonQuery.getQueryOperation(), leftValue, rightValue)) {
                if (jsonQuery.getAnd() != null) {
                    return matchingFilter(parsedJson, queriedValuesCache, Arrays.asList(jsonQuery.getAnd()));
                }
                return matchingFilter(parsedJson, queriedValuesCache, jsonQuery.getOrs());
            }
        }
        return false;
    }

    protected Object findLeftValue(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache,
                                   DataQuery jsonQuery) {
        // CARSTEN unit test
        return retrieveValue(parsedJson, queriedValuesCache, jsonQuery.getLeftType(), jsonQuery.getLeft(), jsonQuery.getHintType());
    }

    protected Object findRightValue(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache,
                                    DataQuery jsonQuery) {
        return retrieveValue(parsedJson, queriedValuesCache, jsonQuery.getRightType(), jsonQuery.getRight(), jsonQuery.getHintType());
    }

    protected Object retrieveValue(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache, FieldType type, Object value, HintType hintType) {
        if (type == FieldType.VALUE) {
            return value;
        }
        String fieldName = (String) value;
        if (queriedValuesCache.containsKey(fieldName)) {
            return queriedValuesCache.get(fieldName);
        }
        String[] split = StringUtils.split(fieldName, '.');
        Object lastObj = parsedJson;
        for (String key : split) {
            if (lastObj != null) {
                Map<String, Object> map = (Map<String, Object>) lastObj;
                lastObj = map.get(key);
            }
        }
        if (hintType == HintType.DATE) {
            if (lastObj instanceof String) {
                try {
                    lastObj = simpleDateFormat.parse((String) lastObj).getTime();
                } catch (java.text.ParseException e) {
                    throw new IllegalArgumentException("Date format does not match the field. ", e);
                }
            } else if (lastObj instanceof Date) {
                lastObj = ((Date) lastObj).getTime();
            }
        }
        queriedValuesCache.put(fieldName, lastObj);
        return lastObj;
    }
/*
    protected byte[] getDataSetFromOffsetsGroup(byte[] buffer, int fromOffset, int datasetLength) {
        byte[] jsonDataset = new byte[datasetLength];
        System.arraycopy(buffer, fromOffset, jsonDataset, 0, datasetLength);
        return jsonDataset;
    } */

    protected Blocks getCompressionBlocksByFile() {
        Cache.ValueWrapper valueWrapper = dataCompressionBlocksCache.get(file);
        if (valueWrapper != null) {
            return (Blocks) valueWrapper.get();
        }
        Blocks blocksByFile = CompressionBlocksUtil.getBlocksByFile(file);
        dataCompressionBlocksCache.put(file, blocksByFile);
        return blocksByFile;
    }

    protected long calculateBlockOffsetUncompressed(long blockIndex, int compressionBlockSize) {
        return blockIndex * compressionBlockSize;
    }

    protected long calculateBlockOffsetCompressed(long blockIndex, List<Integer> compressionBlocks) {
        long result = 0l;
        for (int i = 0; i < blockIndex; i++) {
            result += compressionBlocks.get(i) + getBlockOverhead(); // 4 byte for length of chunk
        }
        return result + getMagicHeaderSize();
    }

    protected abstract int getBlockOverhead();

    protected abstract int getMagicHeaderSize();
}
