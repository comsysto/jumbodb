package org.jumbodb.database.service.query.data.snappy;

import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.data.common.snappy.SnappyUtil;
import org.jumbodb.database.service.query.index.basic.numeric.PseudoCacheForSnappy;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.data.common.snappy.ChunkSkipableSnappyInputStream;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class JsonSnappyRetrieveDataSetsTask implements Callable<Integer> {
    private Logger log = LoggerFactory.getLogger(JsonSnappyRetrieveDataSetsTask.class);

    private final File file;
    private final long fileLength;
    private final JumboQuery searchQuery;
    private final ResultCallback resultCallback;
    private JsonSnappyDataStrategy strategy;
    private final List<FileOffset> offsets;
    //    private final int bufferSize = 10;
//    public final int DEFAULT_BUFFER_SIZE = 32 * 1024;
//    public final int MAXIMUM_OFFSET_GROUP_SIZE = 1000;
    public static final byte[] EMPTY_BUFFER = new byte[0];
//    private final byte[] defaultBuffer = new byte[DEFAULT_BUFFER_SIZE]; // for reuse


    public JsonSnappyRetrieveDataSetsTask(File file, Set<FileOffset> offsets, JumboQuery searchQuery, ResultCallback resultCallback, JsonSnappyDataStrategy strategy) {
        this.file = file;
        this.fileLength = file.length();
        this.searchQuery = searchQuery;
        this.resultCallback = resultCallback;
        this.strategy = strategy;
        this.offsets = new LinkedList<FileOffset>(offsets);
    }

    @Override
    public Integer call() throws Exception {
        Collections.sort(this.offsets);
        JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
        long start = System.currentTimeMillis();
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        ChunkSkipableSnappyInputStream sis = null;
        BufferedReader br = null;
        int results = 0;

        try {
//            List<List<FileOffset>> offsetGroups = groupOffsetsByBufferSize(offsets, DEFAULT_BUFFER_SIZE);
            fis = new FileInputStream(file);
             // ChunkSkipableSnappyInputStream and BufferedInputStream does not work together

            if (searchQuery.getIndexQuery().size() == 0) {
                bis = new BufferedInputStream(fis);
                sis = new ChunkSkipableSnappyInputStream(bis);
                br = new BufferedReader(new InputStreamReader(sis, "UTF-8"));
                log.info("Full scan ");
                long count = 0;
                String line;
                while ((line = br.readLine()) != null && resultCallback.needsMore(searchQuery)) {
                    if (matchingFilter(line, jsonParser, searchQuery.getJsonQuery())) {
                        resultCallback.writeResult(line.getBytes("UTF-8"));
                        results++;
                    }
                    if (count % 100000 == 0) {
                        log.info(file.getAbsolutePath() + ": " + count + " datasets");
                    }
                    count++;
                }
            } else {
                SnappyChunks snappyChunks = PseudoCacheForSnappy.getSnappyChunksByFile(file);
                bis = new BufferedInputStream(fis);
                byte[] readBufferCompressed = new byte[snappyChunks.getChunkSize() * 2];
                byte[] readBufferUncompressed = new byte[snappyChunks.getChunkSize() * 2];
                byte[] resultBuffer = EMPTY_BUFFER;
                long resultBufferStartOffset = 0l;
                long resultBufferEndOffset = 0l;
                byte[] compressedLengthBuffer = new byte[4];
                long uncompressedFileStreamPosition = 0l;
                long compressedFileStreamPosition = 0l;

                for (FileOffset offset : offsets) {
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
                    int lineBreakOffset = resultBuffer.length == 0 ? -1 : findDatasetLengthByLineBreak(resultBuffer, (int)(searchOffset - resultBufferStartOffset));
//                    int datasetStartOffset = (int)(searchOffset - resultBufferStartOffset);
                    while((resultBuffer.length == 0 || lineBreakOffset == -1) && fileLength > compressedFileStreamPosition) {
                        int read1 = bis.read(compressedLengthBuffer);
                        compressedFileStreamPosition += read1;
                        int compressedLength = SnappyUtil.readInt(compressedLengthBuffer, 0);
                        int read = bis.read(readBufferCompressed, 0, compressedLength);
                        compressedFileStreamPosition += read;
                        int uncompressLength = Snappy.uncompress(readBufferCompressed, 0, compressedLength, readBufferUncompressed, 0);
                        uncompressedFileStreamPosition += uncompressLength;
                        int datasetStartOffset = (int)(searchOffset - resultBufferStartOffset);
                        resultBuffer = concat(datasetStartOffset, readBufferUncompressed, resultBuffer, uncompressLength);
                        resultBufferEndOffset = uncompressedFileStreamPosition - 1;
                        resultBufferStartOffset = uncompressedFileStreamPosition - resultBuffer.length; // check right position
//                        datasetStartOffset = (int)(searchOffset - resultBufferStartOffset);
                        lineBreakOffset = findDatasetLengthByLineBreak(resultBuffer, 0);
                    }

                    int datasetLength = lineBreakOffset != -1 ? lineBreakOffset : (resultBuffer.length - 1);

                    byte[] dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(resultBuffer, 0, datasetLength);
//                    byte[] dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(resultBuffer, datasetStartOffset, datasetLength);
                    if (matchingFilter(dataSetFromOffsetsGroup, jsonParser, searchQuery.getJsonQuery())
                            && matchingFilter(dataSetFromOffsetsGroup, jsonParser, offset.getJsonQueries())) {
                        if(!resultCallback.needsMore(searchQuery)) {
                            return results;
                        }
                        resultCallback.writeResult(dataSetFromOffsetsGroup);
                        results++;
                    }
                }
            }
            log.trace("Time for retrieving " + results + " datasets from " + file.getName() + " in " + (System.currentTimeMillis() - start) + "ms");
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
        return results;
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

    protected byte[] concat(byte[] readBuffer, byte[] resultBuffer, int readBufferLength) {
//        if(resultBuffer.length == 0) {
//            return readBuffer;
//        }
        byte[] tmp = new byte[resultBuffer.length + readBufferLength];  // CARSTEN reuse buffer
        System.arraycopy(resultBuffer, 0, tmp, 0, resultBuffer.length);
        System.arraycopy(readBuffer, 0, tmp, resultBuffer.length, readBufferLength);
        return tmp;
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

//    protected boolean matchingFilter(byte[] s, JSONParser jsonParser) throws IOException, ParseException {
//        List<JsonQuery> jsonQueries = searchQuery.getJsonQuery();
//        return matchingFilter(s, jsonParser, jsonQueries);
//    }

    private boolean matchingFilter(byte[] s, JSONParser jsonParser, List<JsonQuery> jsonQueries) throws ParseException, IOException {
        if (jsonQueries.size() == 0) {
            return true;
        }
        String in = new String(s, "UTF-8");
        return matchingFilter(in, jsonParser, jsonQueries);
    }

    private boolean matchingFilter(String s, JSONParser jsonParser, List<JsonQuery> jsonQueries) throws ParseException {
        if (jsonQueries.size() == 0) {
            return true;
        }
        Map<String, Object> parsedJson = (Map<String, Object>)jsonParser.parse(s);
        return matchingFilter(parsedJson, jsonQueries);
    }

    private boolean matchingFilter(Map<String, Object> parsedJson, List<JsonQuery> jsonQueries) throws ParseException {
        if (jsonQueries.size() == 0) {
            return true;
        }
        boolean matching = true;
        for (JsonQuery jsonQuery : jsonQueries) {
            String[] split = StringUtils.split(jsonQuery.getFieldName(), '.');
            Object lastObj = parsedJson;
            for (String key : split) {
                if (lastObj != null) {
                    Map<String, Object> map = (Map<String, Object>) lastObj;
                    lastObj = map.get(key);
                }
            }
            boolean queryClauseMatch = false;
            for (QueryClause queryClause : jsonQuery.getClauses()) {
                if(lastObj != null) {
                    if(strategy.matches(queryClause, lastObj)) {
                        queryClauseMatch = matchingFilter(parsedJson, queryClause.getQueryClauses());
                        break;
                    }
                }
            }
            matching &= queryClauseMatch;
        }
        return matching;
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

    /*
    private byte[] getBufferByOffsetGroup(List<FileOffset> offsetGroup, int chunkSize) {
//        if (offsetGroup.size() == 1) {
//            return defaultBuffer;
//        }
        FileOffset firstFileOffset = offsetGroup.get(0);
        Long first = firstFileOffset.getOffset();
        Long last = offsetGroup.get(offsetGroup.size() - 1).getOffset() + chunkSize;
        int size = (int) (last - first);
        return new byte[size];
    }

    private List<List<FileOffset>> groupOffsetsByBufferSize(List<FileOffset> offsets, int bufSize) {
        List<List<FileOffset>> offsetGroups = new LinkedList<List<FileOffset>>();
        List<FileOffset> group = new ArrayList<FileOffset>();
        int initialOffset = -100000;
        long lastOffset = initialOffset;
        for (FileOffset fileOffset : offsets) {
            long offset = fileOffset.getOffset();
            if ((offset - lastOffset) <= bufSize && group.size() < MAXIMUM_OFFSET_GROUP_SIZE) {
                group.add(fileOffset);
            } else {
                if (lastOffset != initialOffset) {
                    offsetGroups.add(group);
                }
                group = new ArrayList<FileOffset>();
                group.add(fileOffset);
            }

            lastOffset = offset;
        }
        if (!group.isEmpty()) {
            offsetGroups.add(group);
        }
        return offsetGroups;
    }   */
}