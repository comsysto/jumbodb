package org.jumbodb.database.service.query.data.snappy;

import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumbodb.common.query.*;
import org.jumbodb.data.common.snappy.ChunkSkipableSnappyInputStream;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyChunksUtil;
import org.jumbodb.data.common.snappy.SnappyUtil;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.xerial.snappy.Snappy;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public class JsonSnappyLineBreakRetrieveDataSetsTask implements Callable<Integer> {
    private Logger log = LoggerFactory.getLogger(JsonSnappyLineBreakRetrieveDataSetsTask.class);
    private final SimpleDateFormat simpleDateFormat;
    private final boolean resultCacheEnabled;
    private final File file;
    private final Cache datasetsByOffsetsCache;
    private final Cache dataSnappyChunksCache;
    private final boolean scannedSearch;
    private final long fileLength;
    private final JumboQuery searchQuery;
    private final ResultCallback resultCallback;
    private JsonSnappyLineBreakDataStrategy strategy;
    private final List<FileOffset> offsets;
    public static final byte[] EMPTY_BUFFER = new byte[0];
    private int results = 0;
    private JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);


    public JsonSnappyLineBreakRetrieveDataSetsTask(File file, Set<FileOffset> offsets, JumboQuery searchQuery,
      ResultCallback resultCallback, JsonSnappyLineBreakDataStrategy strategy, Cache datasetsByOffsetsCache,
      Cache dataSnappyChunksCache, String dateFormat, boolean scannedSearch) {
        this.file = file;
        this.datasetsByOffsetsCache = datasetsByOffsetsCache;
        this.dataSnappyChunksCache = dataSnappyChunksCache;
        this.scannedSearch = scannedSearch;
        this.fileLength = file.length();
        this.searchQuery = searchQuery;
        this.resultCacheEnabled = searchQuery.isResultCacheEnabled();
        this.resultCallback = resultCallback;
        this.strategy = strategy;
        this.offsets = new LinkedList<FileOffset>(offsets);
        this.simpleDateFormat = new SimpleDateFormat(dateFormat, Locale.US);
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

    // CARSTEN abstract
    private void findLeftDatasetsAndWriteResults(List<FileOffset> leftOffsets) throws ParseException {
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        try {
            fis = new FileInputStream(file);
            SnappyChunks snappyChunks = getSnappyChunksByFile();
            Collections.sort(leftOffsets);
            bis = new BufferedInputStream(fis);
            byte[] readBufferCompressed = new byte[snappyChunks.getChunkSize() * 2];
            byte[] readBufferUncompressed = new byte[snappyChunks.getChunkSize() * 2];
            byte[] resultBuffer = EMPTY_BUFFER;
            long resultBufferStartOffset = 0l;
            long resultBufferEndOffset = 0l;
            byte[] compressedLengthBuffer = new byte[4];
            long uncompressedFileStreamPosition = 0l;
            long compressedFileStreamPosition = 0l;
            for (FileOffset offset : leftOffsets) {
                long searchOffset = offset.getOffset();
                // delete buffer when offset is not inside range and skip
                if (resultBuffer.length == 0 || (resultBufferStartOffset < searchOffset && searchOffset > resultBufferEndOffset)) {
                    long chunkIndex = (searchOffset / snappyChunks.getChunkSize());
                    long chunkOffsetCompressed = calculateChunkOffsetCompressed(chunkIndex, snappyChunks.getChunks());
                    long chunkOffsetUncompressed = calculateChunkOffsetUncompressed(chunkIndex,
                      snappyChunks.getChunkSize());
                    long chunkOffsetToSkip = chunkOffsetCompressed - compressedFileStreamPosition;
                    long skip = skipToOffset(bis, chunkOffsetToSkip);
                    compressedFileStreamPosition += skip;
                    uncompressedFileStreamPosition = chunkOffsetUncompressed;
                    resultBuffer = EMPTY_BUFFER;
                    resultBufferStartOffset = uncompressedFileStreamPosition;
                    resultBufferEndOffset = uncompressedFileStreamPosition;
                }

                // load to result buffer till line break
                int datasetStartOffset = (int) (searchOffset - resultBufferStartOffset);
                int lineBreakOffset = resultBuffer.length == 0 ? -1 : findDatasetLengthByLineBreak(resultBuffer,
                  datasetStartOffset);
                while ((resultBuffer.length == 0 || lineBreakOffset == -1) && fileLength > compressedFileStreamPosition) {
                    int read1 = bis.read(compressedLengthBuffer);
                    compressedFileStreamPosition += read1;
                    int compressedLength = SnappyUtil.readInt(compressedLengthBuffer, 0);
                    int read = bis.read(readBufferCompressed, 0, compressedLength);
                    compressedFileStreamPosition += read;
                    int uncompressLength = Snappy
                      .uncompress(readBufferCompressed, 0, compressedLength, readBufferUncompressed, 0);
                    uncompressedFileStreamPosition += uncompressLength;
                    datasetStartOffset = (int) (searchOffset - resultBufferStartOffset);
                    resultBuffer = concat(datasetStartOffset, readBufferUncompressed, resultBuffer, uncompressLength);
                    resultBufferEndOffset = uncompressedFileStreamPosition - 1;
                    resultBufferStartOffset = uncompressedFileStreamPosition - resultBuffer.length; // check right position
                    datasetStartOffset = 0;
                    lineBreakOffset = findDatasetLengthByLineBreak(resultBuffer, datasetStartOffset);
                }

                int datasetLength = lineBreakOffset != -1 ? lineBreakOffset : (resultBuffer.length - 1 - datasetStartOffset);
                byte[] dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(resultBuffer, datasetStartOffset,
                  datasetLength);
                Map<String, Object> parsedJson = (Map<String, Object>) jsonParser.parse(dataSetFromOffsetsGroup);

                if (resultCacheEnabled) {
                    datasetsByOffsetsCache.put(new CacheFileOffset(file, offset.getOffset()), parsedJson);
                }
                IndexQuery indexQuery = offset.getIndexQuery();
                if (matchingFilter(parsedJson, indexQuery.getAndJson())) {
                    if (!resultCallback.needsMore(searchQuery)) {
                        return;
                    }
                    resultCallback.writeResult(parsedJson);
                    results++;
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(bis);
        }
    }

    private void fullScanData() throws ParseException {
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        ChunkSkipableSnappyInputStream sis = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            sis = new ChunkSkipableSnappyInputStream(bis);
            br = new BufferedReader(new InputStreamReader(sis, "UTF-8"));
            log.info("Full scan");
            long count = 0;
            String line;
            while ((line = br.readLine()) != null && resultCallback.needsMore(searchQuery)) {
                Map<String, Object> parsedJson = (Map<String, Object>) jsonParser.parse(line);
                if (matchingFilter(parsedJson, searchQuery.getDataQuery())) {
                    resultCallback.writeResult(parsedJson);
                    results++;
                }
                if (count % 100000 == 0) {
                    log.info(file.getAbsolutePath() + ": " + count + " datasets");
                }
                count++;
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(bis);
            IOUtils.closeQuietly(sis);
            IOUtils.closeQuietly(br);
        }
    }

    private List<FileOffset> extractOffsetsFromCacheAndWriteResultForExisting() throws IOException, ParseException {
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

    private SnappyChunks getSnappyChunksByFile() {
        Cache.ValueWrapper valueWrapper = dataSnappyChunksCache.get(file);
        if (valueWrapper != null) {
            return (SnappyChunks) valueWrapper.get();
        }
        SnappyChunks snappyChunksByFile = SnappyChunksUtil.getSnappyChunksByFile(file);
        dataSnappyChunksCache.put(file, snappyChunksByFile);
        return snappyChunksByFile;
    }

    // in some cases you have to call skip in a buffered stream multiple times!
    private long skipToOffset(BufferedInputStream bis, long chunkOffsetToSkip) throws IOException {
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
// CARSTEN das ganze matching in abstracte klasse
    private boolean matchingFilter(Map<String, Object> parsedJson, DataQuery jsonQuery) throws ParseException, IOException {
        if (jsonQuery == null) {
            return true;
        }
        return matchingFilter(parsedJson, Arrays.asList(jsonQuery));
    }

    private boolean matchingFilter(Map<String, Object> parsedJson, List<DataQuery> jsonQueries) throws ParseException {
        return matchingFilter(parsedJson, new HashMap<String, Object>(), jsonQueries);
    }

    private boolean matchingFilter(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache,
      List<DataQuery> jsonQueries) throws ParseException {
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
// CARSTEN unit test
    private Object findLeftValue(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache,
      DataQuery jsonQuery) {
        return retrieveValue(parsedJson, queriedValuesCache, jsonQuery.getLeftType(), jsonQuery.getLeft(), jsonQuery.getHintType());
    }

    private Object findRightValue(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache,
      DataQuery jsonQuery) {
        return retrieveValue(parsedJson, queriedValuesCache, jsonQuery.getRightType(), jsonQuery.getRight(), jsonQuery.getHintType());
    }

    private Object retrieveValue(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache, FieldType type, Object value, HintType hintType) {
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
        if(hintType == HintType.DATE) {
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

    private byte[] getDataSetFromOffsetsGroup(byte[] buffer, int fromOffset, int datasetLength) {
        byte[] jsonDataset = new byte[datasetLength];
        System.arraycopy(buffer, fromOffset, jsonDataset, 0, datasetLength);
        return jsonDataset;
    }

    private int findDatasetLengthByLineBreak(byte[] buffer, int fromOffset) {
        int datasetLength = 0;
        boolean found = false;
        for (int i = fromOffset; i < buffer.length; i++) {
            byte aByte = buffer[i];
            if (aByte == 13 || aByte == 10) {
                found = true;
                break;
            }
            datasetLength++;
        }
        if (!found) {
            return -1;
        }
        return datasetLength;
    }
}