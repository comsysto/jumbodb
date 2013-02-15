package core.query;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.xerial.snappy.SnappyInputStream;
import play.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

// CARSTEN extract method as static methods
public class RetrieveDataSetsTask implements Callable<Integer> {

    private final File file;
    private final DumboQuery searchQuery;
    private final ResultCallback resultCallback;
    private final List<Long> offsets;
    private final int bufferSize = 4096;
    private final byte[] defaultBuffer = new byte[bufferSize]; // for reuse


    public RetrieveDataSetsTask(File file, Set<Long> offsets, DumboQuery searchQuery, ResultCallback resultCallback) {
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
        BufferedInputStream bis = null;
        SnappyInputStream lis = null;
        DataInputStream dis = null;
        BufferedReader br = null;
        int results = 0;

        try {

            List<List<Long>> offsetGroups = groupOffsetsByBufferSize(bufferSize);
            System.out.println(file);
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            lis = new SnappyInputStream(bis);
            dis = new DataInputStream(lis);
            long length = dis.readLong();
            br = new BufferedReader(new InputStreamReader(lis));



            if (searchQuery.getIndexComparision().size() == 0) {
                long count = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if (matchingFilter(line, parser)) {
                        resultCallback.writeResult(line);
                        results++;
                    }
                    if (count % 100000 == 0) {
                        Logger.info(file.getAbsolutePath() + ": " + count + " datasets (File length " + length + ")");
                    }
                    count++;
                }
            } else {
                //                long length = fis.available();
                long currentOffset = 8;

                // CARSTEN achtung 8 byte for length)

                for (List<Long> offsetGroup : offsetGroups) {
                    long firstOffset = offsetGroup.get(0);

                    long toSkip = firstOffset - currentOffset + 8;
                    lis.skip(toSkip);
                    currentOffset += toSkip;
                    long available = length - currentOffset;
                    byte[] buffer = getBufferByOffsetGroup(offsetGroup, available);
                    lis.read(buffer);
                    currentOffset += buffer.length;
                    for (Long offset : offsetGroup) {
                        String dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(buffer, (int) (offset - firstOffset));
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
            IOUtils.closeQuietly(bis);
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(lis);
            IOUtils.closeQuietly(br);
        }
        return results;
    }

    private boolean matchingFilter(String s, JSONParser parser) throws IOException, ParseException {
        if (searchQuery.getJsonComparision().size() == 0) {
            return true;
        }
        JSONObject json = (JSONObject) parser.parse(s);
        //    Map<String, Object> json = jsonMapper.readValue(s, Map.class);
        boolean matching = true;
        for (DumboQuery.JsonValueComparision jsonValueComparision : searchQuery.getJsonComparision()) {
            String[] split = StringUtils.split(jsonValueComparision.getName(), '.');
            Object lastObj = json;
            for (String key : split) {
                if (lastObj != null) {
                    Map<String, Object> map = (Map<String, Object>) lastObj;
                    lastObj = map.get(key);
                }
            }
            if (jsonValueComparision.getComparisionType() == DumboQuery.JsonComparisionType.EQUALS &&
                    lastObj != null) {
                // CARSTEN vielleicht ist hier ein hashset schneller?
                matching &= jsonValueComparision.getValues().contains(lastObj);
            } else if (jsonValueComparision.getComparisionType() == DumboQuery.JsonComparisionType.EQUALS_IGNORE_CASE) {
                throw new IllegalArgumentException("Not yet implemented " + jsonValueComparision.getComparisionType());
            } else {
                throw new IllegalArgumentException("Unsupported comparision type " + jsonValueComparision.getComparisionType());
            }
        }
        return matching;
    }

    private String getDataSetFromOffsetsGroup(byte[] buffer, int relativeOffset) {
        int pos = 0;
        for (int i = relativeOffset; i < buffer.length; i++) {
            byte aByte = buffer[i];
            if (aByte == 10 || aByte == 13) {
                break;
            }
            pos++;
        }
        String s = new String(buffer, relativeOffset, pos);
        return s;
    }

    private byte[] getBufferByOffsetGroup(List<Long> offsetGroup, long available) {
//            if(available < bufferSize) {
//                return new byte[(int)available];
//            }
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