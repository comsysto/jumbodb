package org.jumbodb.database.service.query.data.snappy;

import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.snappy.ChunkSkipableSnappyInputStream;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyChunksUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class JsonSnappyRetrieveDataSetsTask implements Callable<Integer> {
    private Logger log = LoggerFactory.getLogger(JsonSnappyRetrieveDataSetsTask.class);

    private final File file;
    private final JumboQuery searchQuery;
    private final ResultCallback resultCallback;
    private JsonSnappyDataStrategy strategy;
    private final List<Long> offsets;
    //    private final int bufferSize = 10;
    private final int bufferSize = 16 * 1024; // CARSTEN make the buffer size learnable by collection
    private final int maximumOffsetGroupSize = 1000;
    public static final byte[] EMPTY_BUFFER = new byte[0];
    private final byte[] defaultBuffer = new byte[bufferSize]; // for reuse


    public JsonSnappyRetrieveDataSetsTask(File file, Set<Long> offsets, JumboQuery searchQuery, ResultCallback resultCallback, JsonSnappyDataStrategy strategy) {
        this.file = file;
        this.searchQuery = searchQuery;
        this.resultCallback = resultCallback;
        this.strategy = strategy;
        this.offsets = new LinkedList<Long>(offsets);
    }

    @Override
    public Integer call() throws Exception {
        Collections.sort(this.offsets);
        JSONParser jsonParser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
        long start = System.currentTimeMillis();
        FileInputStream fis = null;
        ChunkSkipableSnappyInputStream sis = null;
        DataInputStream dis = null;
        BufferedReader br = null;
        FileInputStream chunksFis = null;
        DataInputStream chunksDis = null;
        int results = 0;

        try {

            List<List<Long>> offsetGroups = groupOffsetsByBufferSize(bufferSize);
            fis = new FileInputStream(file);
             // ChunkSkipableSnappyInputStream and BufferedInputStream does not work together

            if (searchQuery.getIndexQuery().size() == 0) {
                sis = new ChunkSkipableSnappyInputStream(new BufferedInputStream(fis));
                br = new BufferedReader(new InputStreamReader(sis));
                log.info("Full scan ");
                long count = 0;
                String line;
                while ((line = br.readLine()) != null && resultCallback.needsMore()) {
                    if (matchingFilter(line, jsonParser)) {
                        resultCallback.writeResult(line.getBytes());
                        results++;
                    }
                    if (count % 100000 == 0) {
                        log.info(file.getAbsolutePath() + ": " + count + " datasets");
                    }
                    count++;
                }
            } else {
                sis = new ChunkSkipableSnappyInputStream(fis);
                dis = new DataInputStream(sis);
                SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(file);

                long currentOffset = 0;
                byte[] lastBuffer = EMPTY_BUFFER;
                for (List<Long> offsetGroup : offsetGroups) {
                    long firstOffset = offsetGroup.get(0);
                    long toSkip = firstOffset - currentOffset;

                    // CARSTEN make this system outs trace
//                    System.out.println(file.getAbsolutePath());
                    long chunkIndex = (firstOffset / snappyChunks.getChunkSize());
//                    System.out.println("chunkIndex " + chunkIndex );
                    long chunkOffsetCompressed = calculateChunkOffsetCompressed(chunkIndex, snappyChunks.getChunks());
//                    System.out.println("chunkOffsetCompressed " + chunkOffsetCompressed );
                    long chunkOffsetUncompressed = calculateChunkOffsetUncompressed(chunkIndex, snappyChunks.getChunkSize());
//                    System.out.println("chunkOffsetUncompressed " + chunkOffsetUncompressed );
                    long position = fis.getChannel().position();
//                    System.out.println("position " + position );
                    long chunkOffsetToSkip = chunkOffsetCompressed - position;
//                    System.out.println("chunkOffsetToSkip " + chunkOffsetToSkip );
                    if(chunkOffsetToSkip > 0) {
                        // other chunk
                        sis.skipCompressed(chunkOffsetToSkip);
                        long partialSkipInData = firstOffset - chunkOffsetUncompressed;
//                    System.out.println("partialSkipInData " + partialSkipInData );
                        sis.skip(partialSkipInData);
                    }
                    else if(toSkip > 0) {
                        // same chunk
                        sis.skip(toSkip);
                    }
                    if(toSkip >= 0) {
                        currentOffset += toSkip;
                    }
                    // CARSTEN reuse buffer and let grow
                    byte[] readBuffer = getBufferByOffsetGroup(offsetGroup);
                    byte[] resultBuffer = getResultBuffer(lastBuffer, toSkip);

                    boolean foundEnd =  false; // line break or EOF
                    while(!foundEnd) {
                        int read = sis.read(readBuffer);
                        if(read != -1) {
                        }
                        if(readBuffer.length == read) {
                            foundEnd = findDatasetLengthByLineBreak(readBuffer, 0) != -1;

                        } else {
                            foundEnd = true;
                        }
                        if(read != -1) {
                            currentOffset += read;
                            resultBuffer = concat(readBuffer, resultBuffer, read);
                        }
//                        System.out.println(new String(resultBuffer) + " read " + read + " readBuffer.length " + readBuffer.length);
                    }
                    lastBuffer = resultBuffer;

                    for (Long offset : offsetGroup) {
                        int fromOffset = (int) (offset - firstOffset);
                        int datasetLength = findDatasetLengthByLineBreak(resultBuffer, fromOffset);
                        byte[] dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(resultBuffer, fromOffset, datasetLength);
                        if (matchingFilter(dataSetFromOffsetsGroup, jsonParser)) {
                            if(!resultCallback.needsMore()) {
                                return results;
                            }
                            resultCallback.writeResult(dataSetFromOffsetsGroup);
                            results++;
                        }
                    }
                }
            }
            log.info("Time for retrieving " + results + " datasets: " + (System.currentTimeMillis() - start) + "ms");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(sis);
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(chunksDis);
            IOUtils.closeQuietly(chunksFis);
        }
        return results;
    }

    private byte[] getResultBuffer(byte[] lastBuffer, long toSkip) {
        if(toSkip >= 0) {
            return EMPTY_BUFFER;
        }
        int length = Math.abs((int)toSkip);
        byte[] last = new byte[length];
        System.arraycopy(lastBuffer, lastBuffer.length - length, last, 0, length);
        return last;
    }

    private byte[] concat(byte[] readBuffer, byte[] resultBuffer, int readBufferLength) {
        byte[] tmp = new byte[resultBuffer.length + readBufferLength];
        System.arraycopy(resultBuffer, 0, tmp, 0, resultBuffer.length);
        System.arraycopy(readBuffer, 0, tmp, resultBuffer.length, readBufferLength);
        return tmp;
    }

    private long calculateChunkOffsetUncompressed(long chunkIndex, int snappyChunkSize) {
//        long l = chunkIndex - 1;
//        return l > 0 ? l * ChunkSkipableSnappyInputStream.snappyChunkSize : 0;
        return chunkIndex * snappyChunkSize;
    }

    private long calculateChunkOffsetCompressed(long chunkIndex, List<Integer> snappyChunks) {
        long result = 0l;
        for(int i = 0; i < chunkIndex; i++) {
            result += snappyChunks.get(i) + 4; // 4 byte for length of chunk
        }
        return result + 16;
    }

    private boolean matchingFilter(byte[] s, JSONParser jsonParser) throws IOException, ParseException {
        return matchingFilter(new String(s), jsonParser);
    }

    private boolean matchingFilter(String s, JSONParser jsonParser) throws IOException, ParseException {
        if (searchQuery.getJsonQuery().size() == 0) {
            return true;
        }
//        JSONObject cl = new JSONObject();

        boolean matching = true;
        for (JsonQuery jsonQuery : searchQuery.getJsonQuery()) {
            String[] split = StringUtils.split(jsonQuery.getFieldName(), '.');
//            UpdaterMapper<JSONObject> mapper = new UpdaterMapper<JSONObject>(cl);
            Object lastObj = jsonParser.parse(s);
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
                        queryClauseMatch = true;
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

    private byte[] getBufferByOffsetGroup(List<Long> offsetGroup) {
        if (offsetGroup.size() == 1) {
            return defaultBuffer;
        }
        Long first = offsetGroup.get(0);
        Long last = offsetGroup.get(offsetGroup.size() - 1) + bufferSize;
        int size = (int) (last - first);
        return new byte[size];

    }

    private List<List<Long>> groupOffsetsByBufferSize(int bufSize) {
        List<List<Long>> offsetGroups = new LinkedList<List<Long>>();
        List<Long> group = new ArrayList<Long>();
        int initialOffset = -100000;
        long lastOffset = initialOffset;
        for (Long offset : offsets) {

            if ((offset - lastOffset) <= bufSize && group.size() < maximumOffsetGroupSize) {
                group.add(offset);
            } else {
                if (lastOffset != initialOffset) {
                    offsetGroups.add(group);
                }
                group = new ArrayList<Long>();
                group.add(offset);
            }

            lastOffset = offset;
        }
        if (!group.isEmpty()) {
            offsetGroups.add(group);
        }
        return offsetGroups;
    }
}