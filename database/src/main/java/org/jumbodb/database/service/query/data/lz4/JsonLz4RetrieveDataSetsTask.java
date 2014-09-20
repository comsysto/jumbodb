package org.jumbodb.database.service.query.data.lz4;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jpountz.lz4.LZ4BlockInputStream;
import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.data.common.compression.CompressionUtil;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.common.DefaultRetrieveDataSetsTask;
import org.jumbodb.database.service.query.data.snappy.CacheFileOffset;
import org.springframework.cache.Cache;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonLz4RetrieveDataSetsTask extends DefaultRetrieveDataSetsTask {

    private ObjectMapper jsonParser = new ObjectMapper();

    public JsonLz4RetrieveDataSetsTask(File file, Set<FileOffset> offsets, JumboQuery searchQuery,
                                       ResultCallback resultCallback, DataStrategy strategy, Cache datasetsByOffsetsCache,
                                       String dateFormat, boolean scannedSearch) {
        super(datasetsByOffsetsCache, resultCallback, strategy, dateFormat, offsets, searchQuery, file, scannedSearch);
    }

    @Override
    protected void findLeftDatasetsAndWriteResults(List<FileOffset> leftOffsets) {
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        LZ4BlockInputStream lz4Is = null;
        DataInputStream dis = null;
        try {
            Collections.sort(leftOffsets);
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            lz4Is = new LZ4BlockInputStream(bis);
            dis = new DataInputStream(lz4Is);
            long currentOffset = 0;
            byte[] lengthBuffer = new byte[4];
            byte[] data = new byte[0];
            for (FileOffset offset : leftOffsets) {
                long searchOffset = offset.getOffset();
                while(searchOffset != currentOffset) {
                    long skipped = lz4Is.skip(searchOffset - currentOffset);
                    currentOffset += skipped;
                }
                dis.readFully(lengthBuffer);
                currentOffset += 4;
                int length = CompressionUtil.readInt(lengthBuffer, 0);
                if (data.length < length) {
                    data = new byte[length];
                }
                dis.readFully(data, 0, length);
                currentOffset += length;
                Map<String, Object> parsedJson = (Map<String, Object>) jsonParser.readValue(data, 0, length, Map.class);

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
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(lz4Is);
            IOUtils.closeQuietly(bis);
            IOUtils.closeQuietly(fis);
        }
    }

    @Override
    protected void fullScanData() {
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        LZ4BlockInputStream sis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            sis = new LZ4BlockInputStream(bis);
            dis = new DataInputStream(sis);
            log.info("Full scan");
            long count = 0;
            int length;
            byte[] data = new byte[0];
            while ((length = dis.readInt()) != -1  && resultCallback.needsMore(searchQuery)) {
                if (data.length < length) {
                    data = new byte[length];
                }
                dis.readFully(data, 0, length);
                Map<String, Object> parsedJson = (Map<String, Object>) jsonParser.readValue(data, 0, length, Map.class);
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
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(bis);
            IOUtils.closeQuietly(sis);
        }
    }
}