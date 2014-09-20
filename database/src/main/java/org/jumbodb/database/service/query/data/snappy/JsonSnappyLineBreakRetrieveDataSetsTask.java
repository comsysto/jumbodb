package org.jumbodb.database.service.query.data.snappy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.*;
import org.jumbodb.data.common.snappy.ChunkSkipableSnappyInputStream;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyUtil;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.springframework.cache.Cache;
import org.xerial.snappy.Snappy;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class JsonSnappyLineBreakRetrieveDataSetsTask extends AbstractSnappyRetrieveDataSetsTask {
    private ObjectMapper jsonParser = new ObjectMapper();

    public JsonSnappyLineBreakRetrieveDataSetsTask(File file, Set<FileOffset> offsets, JumboQuery searchQuery,
      ResultCallback resultCallback, DataStrategy strategy, Cache datasetsByOffsetsCache,
      Cache dataSnappyChunksCache, String dateFormat, boolean scannedSearch) {
        super(datasetsByOffsetsCache, resultCallback, strategy, dateFormat, offsets, searchQuery, file, scannedSearch, dataSnappyChunksCache);
    }

    @Override
    protected void findLeftDatasetsAndWriteResults(List<FileOffset> leftOffsets) {
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
                    compressedFileStreamPosition += bis.read(compressedLengthBuffer);
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
            throw new RuntimeException(e);
        } catch (IOException e) {
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
                Map<String, Object> parsedJson = jsonParser.readValue(line, Map.class);
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