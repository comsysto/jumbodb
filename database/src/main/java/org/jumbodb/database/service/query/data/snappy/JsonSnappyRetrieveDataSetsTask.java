package org.jumbodb.database.service.query.data.snappy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.data.common.compression.CompressionUtil;
import org.jumbodb.data.common.snappy.ChunkSkipableSnappyInputStream;
import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.common.DefaultRetrieveDataSetsTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonSnappyRetrieveDataSetsTask extends DefaultRetrieveDataSetsTask {
    private static Logger log = LoggerFactory.getLogger(JsonSnappyRetrieveDataSetsTask.class);


    private ObjectMapper jsonParser = new ObjectMapper();

    public JsonSnappyRetrieveDataSetsTask(File file, Set<FileOffset> offsets, JumboQuery searchQuery,
                                          ResultCallback resultCallback, DataStrategy strategy, Cache datasetsByOffsetsCache,
                                          Cache dataCompressionBlocksCache, String dateFormat, boolean scannedSearch) {
        super(datasetsByOffsetsCache, dataCompressionBlocksCache, resultCallback, strategy, dateFormat, offsets, searchQuery, file, scannedSearch);
    }

    @Override
    protected void findLeftDatasetsAndWriteResults(List<FileOffset> leftOffsets) {
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        try {
            fis = new FileInputStream(file);
            Blocks blocks = getCompressionBlocksByFile();
            Collections.sort(leftOffsets);
            bis = new BufferedInputStream(fis);
            byte[] readBufferCompressed = new byte[blocks.getBlockSize() * 2];
            byte[] readBufferUncompressed = new byte[blocks.getBlockSize() * 2];
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
                    long chunkIndex = (searchOffset / blocks.getBlockSize());
                    long chunkOffsetCompressed = calculateBlockOffsetCompressed(chunkIndex, blocks.getBlocks());
                    long chunkOffsetUncompressed = calculateBlockOffsetUncompressed(chunkIndex,
                            blocks.getBlockSize());
                    long chunkOffsetToSkip = chunkOffsetCompressed - compressedFileStreamPosition;
                    long skip = skipToOffset(bis, chunkOffsetToSkip);
                    compressedFileStreamPosition += skip;
                    uncompressedFileStreamPosition = chunkOffsetUncompressed;
                    resultBuffer = EMPTY_BUFFER;
                    resultBufferStartOffset = uncompressedFileStreamPosition;
                    resultBufferEndOffset = uncompressedFileStreamPosition;
                }

                int datasetStartOffset = (int) (searchOffset - resultBufferStartOffset);
                int datasetLength = Integer.MIN_VALUE;
                if (resultBuffer.length > 0) {
                    datasetLength = CompressionUtil.readInt(resultBuffer, datasetStartOffset);
                    datasetStartOffset += 4; // int length
                }
                while ((resultBuffer.length == 0 || datasetLength > (resultBuffer.length - datasetStartOffset))
                        && datasetLength != -1) {
                    compressedFileStreamPosition += bis.read(compressedLengthBuffer);
                    int compressedLength = CompressionUtil.readInt(compressedLengthBuffer, 0);
                    int read = bis.read(readBufferCompressed, 0, compressedLength);
                    compressedFileStreamPosition += read;
                    int uncompressLength = Snappy
                            .uncompress(readBufferCompressed, 0, compressedLength, readBufferUncompressed, 0);
                    uncompressedFileStreamPosition += uncompressLength;
                    datasetStartOffset = (int) (searchOffset - resultBufferStartOffset);
                    resultBuffer = concat(datasetStartOffset, readBufferUncompressed, resultBuffer, uncompressLength);
                    resultBufferEndOffset = uncompressedFileStreamPosition; // warum war hier + 1?
                    resultBufferStartOffset = uncompressedFileStreamPosition - resultBuffer.length; // check right position
                    datasetStartOffset = 0;
                    if (resultBuffer.length > 0) {
                        datasetLength = CompressionUtil.readInt(resultBuffer, datasetStartOffset);
                        datasetStartOffset += 4; // int length
                    } else {
                        datasetLength = Integer.MIN_VALUE;
                    }
                }
                // end load result buffer til line break

//                int datasetLength = lineBreakOffset != -1 ? lineBreakOffset : (resultBuffer.length - 1 - datasetStartOffset);
                byte[] dataSetFromOffsetsGroup = getDataSetFromOffsetsGroup(resultBuffer, datasetStartOffset,
                        datasetLength);
                Map<String, Object> parsedJson = jsonParser.readValue(dataSetFromOffsetsGroup, Map.class);

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
            log.error("Error", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("Error", e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(bis);
        }
    }

    @Override
    protected void fullScanData() {
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        ChunkSkipableSnappyInputStream sis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            sis = new ChunkSkipableSnappyInputStream(bis);
            dis = new DataInputStream(sis);
            log.info("Full scan");
            long count = 0;
            int length;
            byte[] data = new byte[0];
            while ((length = dis.readInt()) != -1 && resultCallback.needsMore(searchQuery)) {
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
            log.error("Error", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("Error", e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(bis);
            IOUtils.closeQuietly(sis);
        }
    }

    @Override
    protected int getMagicHeaderSize() {
        return 16;
    }

    @Override
    protected int getBlockOverhead() {
        return 4;
    }
}