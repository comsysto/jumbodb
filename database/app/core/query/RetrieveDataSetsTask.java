package core.query;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import play.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class RetrieveDataSetsTask implements Callable<Integer> {

    private final File file;
    private final JumboQuery searchQuery;
    private final ResultCallback resultCallback;
    private final List<Long> offsets;
    private final int bufferSize = 4096;
    private final byte[] defaultBuffer = new byte[bufferSize]; // for reuse


    public RetrieveDataSetsTask(File file, Set<Long> offsets, JumboQuery searchQuery, ResultCallback resultCallback) {
        this.file = file;
        this.searchQuery = searchQuery;
        this.resultCallback = resultCallback;
        this.offsets = new LinkedList<Long>(offsets);
    }

    @Override
    public Integer call() throws Exception {
        JSONParser parser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
        Collections.sort(this.offsets);
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
            sis = new ChunkSkipableSnappyInputStream(fis);
            dis = new DataInputStream(sis);
            SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(file);

            br = new BufferedReader(new InputStreamReader(sis));


            if (searchQuery.getIndexComparision().size() == 0) {
                Logger.info("Full scan");
                long count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    Logger.info("Line " + line);
                    // CARSTEN fix this doubled conversion  -> String to byte, read bytes directly
                    byte[] lineBytes = line.getBytes();
                    if (matchingFilter(lineBytes, parser)) {
                        resultCallback.writeResult(lineBytes);
                        results++;
                    }
                    if (count % 100000 == 0) {
                        Logger.info(file.getAbsolutePath() + ": " + count + " datasets (File length " + snappyChunks.getLength() + ")");
                    }
                    count++;
                }
            } else {
                long currentOffset = 0;
                for (List<Long> offsetGroup : offsetGroups) {
                    long firstOffset = offsetGroup.get(0);
                    long toSkip = firstOffset - currentOffset;

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
                    else {
                        // same chunk
                        sis.skip(toSkip);
                    }

                    currentOffset += toSkip;
                    long available = snappyChunks.getLength() - currentOffset;
                    // CARSTEN reuse buffer and let grow
                    // CARSTEN support bigger documents
                    byte[] buffer = getBufferByOffsetGroup(offsetGroup, available);
                    sis.read(buffer);
                    currentOffset += buffer.length;
                    for (Long offset : offsetGroup) {
                        byte[] dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(buffer, (int) (offset - firstOffset));
                        if (matchingFilter(dataSetFromOffsetsGroup, parser)) {
                            resultCallback.writeResult(dataSetFromOffsetsGroup);
                            results++;
                        }
                    }
                }
            }
            Logger.info("Time for retrieving " + results + " datasets: " + (System.currentTimeMillis() - start) + "ms");
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

    private boolean matchingFilter(byte[] s, JSONParser parser) throws IOException, ParseException {
        if (searchQuery.getJsonComparision().size() == 0) {
            return true;
        }
        JSONObject json = (JSONObject) parser.parse(s);
        //    Map<String, Object> json = jsonMapper.readValue(s, Map.class);
        boolean matching = true;
        for (JumboQuery.JsonValueComparision jsonValueComparision : searchQuery.getJsonComparision()) {
            String[] split = StringUtils.split(jsonValueComparision.getName(), '.');
            Object lastObj = json;
            for (String key : split) {
                if (lastObj != null) {
                    Map<String, Object> map = (Map<String, Object>) lastObj;
                    lastObj = map.get(key);
                }
            }
            if (jsonValueComparision.getComparisionType() == JumboQuery.JsonComparisionType.EQUALS &&
                    lastObj != null) {
                matching &= jsonValueComparision.getValues().contains(lastObj);
            } else if (jsonValueComparision.getComparisionType() == JumboQuery.JsonComparisionType.EQUALS_IGNORE_CASE) {
                throw new IllegalArgumentException("Not yet implemented " + jsonValueComparision.getComparisionType());
            } else {
                throw new IllegalArgumentException("Unsupported comparision type " + jsonValueComparision.getComparisionType());
            }
        }
        return matching;
    }

    private byte[] getDataSetFromOffsetsGroup(byte[] buffer, int relativeOffset) {
        int pos = 0;
        for (int i = relativeOffset; i < buffer.length; i++) {
            byte aByte = buffer[i];
            if (aByte == 10 || aByte == 13) {
                break;
            }
            pos++;
        }
        // CARSTEN fixen
        byte[] result = new byte[pos];
        System.arraycopy(buffer, relativeOffset, result, 0, pos);
        return result;
//        String s = new String(buffer, relativeOffset, pos);
//        return s;
    }

    private byte[] getBufferByOffsetGroup(List<Long> offsetGroup, long available) {
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

            if ((offset - lastOffset) <= bufSize) {
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