package org.jumbodb.database.service.query.data.snappy;

import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.data.common.snappy.SnappyChunksUtil;
import org.jumbodb.data.common.snappy.SnappyUtil;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.data.common.snappy.ChunkSkipableSnappyInputStream;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class JsonSnappyRetrieveDataSetsTask implements Callable<Integer> {
    private Logger log = LoggerFactory.getLogger(JsonSnappyRetrieveDataSetsTask.class);

    private final boolean resultCacheEnabled;
    private final File file;
    private final Cache datasetsByOffsetsCache;
    private final Cache dataSnappyChunksCache;
    private final long fileLength;
    private final JumboQuery searchQuery;
    private final ResultCallback resultCallback;
    private JsonSnappyDataStrategy strategy;
    private final List<FileOffset> offsets;
    public static final byte[] EMPTY_BUFFER = new byte[0];
    private int results = 0;
    private JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);


    public JsonSnappyRetrieveDataSetsTask(File file, Set<FileOffset> offsets, JumboQuery searchQuery, ResultCallback resultCallback, JsonSnappyDataStrategy strategy, Cache datasetsByOffsetsCache, Cache dataSnappyChunksCache) {
        this.file = file;
        this.datasetsByOffsetsCache = datasetsByOffsetsCache;
        this.dataSnappyChunksCache = dataSnappyChunksCache;
        this.fileLength = file.length();
        this.searchQuery = searchQuery;
        this.resultCacheEnabled = searchQuery.isResultCacheEnabled();
        this.resultCallback = resultCallback;
        this.strategy = strategy;
        this.offsets = new LinkedList<FileOffset>(offsets);
    }

    @Override
    public Integer call() throws Exception {
        if (!searchQuery.getJsonQuery().isEmpty()) {
            fullScanData();
        } else {
            long start = System.currentTimeMillis();
            if(resultCacheEnabled) {
                List<FileOffset> leftOffsets = extractOffsetsFromCacheAndWriteResultForExisting();
                if(leftOffsets.isEmpty()) {
                    log.trace("Time for retrieving " + results + " only from cache " + file.getName() + " in " + (System.currentTimeMillis() - start) + "ms");
                    return results;
                }
                int cacheResult = results;
                findLeftDatasetsAndWriteResults(leftOffsets);
                log.trace("Time for retrieving " + results + " (" + cacheResult + " from cache) datasets from " + file.getName() + " in " + (System.currentTimeMillis() - start) + "ms");

            }
            else {
                findLeftDatasetsAndWriteResults(offsets);
                log.trace("Time for retrieving " + results + " (caching disabled) datasets from " + file.getName() + " in " + (System.currentTimeMillis() - start) + "ms");
            }
        }
        return results;
    }

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
                if(resultBuffer.length == 0 || (resultBufferStartOffset < searchOffset && searchOffset > resultBufferEndOffset)) {
                    long chunkIndex = (searchOffset / snappyChunks.getChunkSize());
                    long chunkOffsetCompressed = calculateChunkOffsetCompressed(chunkIndex, snappyChunks.getChunks());
                    long chunkOffsetUncompressed = calculateChunkOffsetUncompressed(chunkIndex, snappyChunks.getChunkSize());
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
                int lineBreakOffset = resultBuffer.length == 0 ? -1 : findDatasetLengthByLineBreak(resultBuffer, datasetStartOffset);
//                    int lineBreakOffset = resultBuffer.length == 0 ? -1 : findDatasetLengthByLineBreak(resultBuffer, (int)(searchOffset - resultBufferStartOffset));
//                    int datasetStartOffset = (int)(searchOffset - resultBufferStartOffset);
                while((resultBuffer.length == 0 || lineBreakOffset == -1) && fileLength > compressedFileStreamPosition) {
                    int read1 = bis.read(compressedLengthBuffer);
                    compressedFileStreamPosition += read1;
                    int compressedLength = SnappyUtil.readInt(compressedLengthBuffer, 0);
                    int read = bis.read(readBufferCompressed, 0, compressedLength);
                    compressedFileStreamPosition += read;
                    int uncompressLength = Snappy.uncompress(readBufferCompressed, 0, compressedLength, readBufferUncompressed, 0);
                    uncompressedFileStreamPosition += uncompressLength;
                    datasetStartOffset = (int)(searchOffset - resultBufferStartOffset);
                    resultBuffer = concat(datasetStartOffset, readBufferUncompressed, resultBuffer, uncompressLength);
//                        resultBuffer = concat(readBufferUncompressed, resultBuffer, uncompressLength);
                    resultBufferEndOffset = uncompressedFileStreamPosition - 1;
                    resultBufferStartOffset = uncompressedFileStreamPosition - resultBuffer.length; // check right position
                    datasetStartOffset = 0;
                    lineBreakOffset = findDatasetLengthByLineBreak(resultBuffer, datasetStartOffset);
                }

//                    int datasetLength = lineBreakOffset != -1 ? lineBreakOffset : resultBuffer.length ;
                int datasetLength = lineBreakOffset != -1 ? lineBreakOffset : (resultBuffer.length - 1 - datasetStartOffset);
//                    byte[] dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(resultBuffer, 0, datasetLength);
                byte[] dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(resultBuffer, datasetStartOffset, datasetLength);
                if(resultCacheEnabled) {
                    datasetsByOffsetsCache.put(new CacheFileOffset(file, offset.getOffset()), dataSetFromOffsetsGroup);
                }
                IndexQuery indexQuery = offset.getIndexQuery();
                if (matchingFilter(dataSetFromOffsetsGroup, indexQuery.getAndJson())) {
                    if(!resultCallback.needsMore(searchQuery)) {
                        return;
                    }
                    resultCallback.writeResult(dataSetFromOffsetsGroup);
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
                // CARSTEN hier mit OR auch das FileOffset checken falls Index und Json Query am Root level abgefragt werden
                if (matchingFilter(line, searchQuery.getJsonQuery())) {
                    resultCallback.writeResult(line.getBytes("UTF-8"));
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
            if(valueWrapper != null) {
                byte[] dataSetFromOffsetsGroup = (byte[]) valueWrapper.get();
                IndexQuery indexQuery = offset.getIndexQuery();
                if (matchingFilter(dataSetFromOffsetsGroup, indexQuery.getAndJson())) {
                    if(!resultCallback.needsMore(searchQuery)) {
                        return Collections.emptyList(); // return empty list enough found!
                    }
                    resultCallback.writeResult(dataSetFromOffsetsGroup);
                    results++;
                }
            }
            else {
                notFoundOffsets.add(offset);
            }
        }
        return notFoundOffsets;
    }

    private SnappyChunks getSnappyChunksByFile() {
        Cache.ValueWrapper valueWrapper = dataSnappyChunksCache.get(file);
        if(valueWrapper != null) {
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
        while(skipped != chunkOffsetToSkip || lastSkipped != 0l) {
            lastSkipped = bis.skip(chunkOffsetToSkip - skipped);
            skipped += lastSkipped;
        }
        return skipped;
    }

    protected byte[] getResultBuffer(byte[] lastBuffer, long toSkip) {
        if(toSkip >= 0) {
            return EMPTY_BUFFER;
        }
        int length = Math.abs((int)toSkip);
        byte[] last = new byte[length];
        System.arraycopy(lastBuffer, lastBuffer.length - length, last, 0, length);
        return last;
    }

    protected byte[] concat(int datasetStartOffset, byte[] readBuffer, byte[] resultBuffer, int readBufferLength) {
        int resBufLen = resultBuffer.length - datasetStartOffset;
        byte[] tmp = new byte[resBufLen + readBufferLength];
        if(resBufLen < 0) {
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
        for(int i = 0; i < chunkIndex; i++) {
            result += snappyChunks.get(i) + 4; // 4 byte for length of chunk
        }
        return result + 16;
    }
    private boolean matchingFilter(byte[] s, JsonQuery jsonQuery) throws ParseException, IOException {
        if(jsonQuery == null) {
            return true;
        }
        return matchingFilter(s, Arrays.asList(jsonQuery));
    }

    private boolean matchingFilter(byte[] s, List<JsonQuery> jsonQueries) throws ParseException, IOException {
        if (jsonQueries.size() == 0) {
            return true;
        }
        String in = new String(s, "UTF-8");
        return matchingFilter(in, jsonQueries);
    }

    private boolean matchingFilter(String s, List<JsonQuery> jsonQueries) throws ParseException {
        if (jsonQueries.size() == 0) {
            return true;
        }
        Map<String, Object> parsedJson = (Map<String, Object>)jsonParser.parse(s);
        return matchingFilter(parsedJson, new HashMap<String, Object>(), jsonQueries);
    }

    private boolean matchingFilter(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache, List<JsonQuery> jsonQueries) throws ParseException {
        if (jsonQueries.size() == 0) {
            return true;
        }
        for (JsonQuery jsonQuery : jsonQueries) {
            Object lastObj = findValueFromJson(parsedJson, queriedValuesCache, jsonQuery);
            if(lastObj != null) {
                if(strategy.matches(jsonQuery, lastObj)) {
                    if(jsonQuery.getAnd() != null) {
                        return matchingFilter(parsedJson, queriedValuesCache, Arrays.asList(jsonQuery.getAnd()));
                    }
                    return matchingFilter(parsedJson, queriedValuesCache, jsonQuery.getOrs());
                }
            }
        }
        return false;
    }

    private Object findValueFromJson(Map<String, Object> parsedJson, Map<String, Object> queriedValuesCache, JsonQuery jsonQuery) {
        if(queriedValuesCache.containsKey(jsonQuery.getFieldName())) {
            return queriedValuesCache.get(jsonQuery.getFieldName());
        }
        String[] split = StringUtils.split(jsonQuery.getFieldName(), '.');
        Object lastObj = parsedJson;
        for (String key : split) {
            if (lastObj != null) {
                Map<String, Object> map = (Map<String, Object>) lastObj;
                lastObj = map.get(key);
            }
        }
        queriedValuesCache.put(jsonQuery.getFieldName(), lastObj);
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
        if(!found) {
            return -1;
        }
        return datasetLength;
    }
}