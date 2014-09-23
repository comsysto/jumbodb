package org.jumbodb.database.service.query.data.lz4;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.Utils;
import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.data.common.compression.CompressionUtil;
import org.jumbodb.data.common.lz4.LZ4BlockInputStream;
import org.jumbodb.data.common.snappy.ChunkSkipableSnappyInputStream;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.common.DefaultRetrieveDataSetsTask;
import org.jumbodb.database.service.query.data.snappy.CacheFileOffset;
import org.springframework.cache.Cache;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonLz4LineBreakRetrieveDataSetsTask extends DefaultRetrieveDataSetsTask {
    private ObjectMapper jsonParser = new ObjectMapper();

    public JsonLz4LineBreakRetrieveDataSetsTask(File file, Set<FileOffset> offsets, JumboQuery searchQuery,
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
            byte[] lengthsBuffer = new byte[8];
            long uncompressedFileStreamPosition = 0l;
            long compressedFileStreamPosition = 0l;
            LZ4Factory factory = LZ4Factory.fastestInstance();
            LZ4FastDecompressor decompressor = factory.fastDecompressor();
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

                // load to result buffer till line break
                int datasetStartOffset = (int) (searchOffset - resultBufferStartOffset);
                int lineBreakOffset = resultBuffer.length == 0 ? -1 : findDatasetLengthByLineBreak(resultBuffer,
                        datasetStartOffset);
                while ((resultBuffer.length == 0 || lineBreakOffset == -1) && fileLength > compressedFileStreamPosition) {
                    compressedFileStreamPosition += bis.read(lengthsBuffer);
                    int compressedLength = Utils.readIntLE(lengthsBuffer, 0);
                    int uncompressedLength = Utils.readIntLE(lengthsBuffer, 4);
                    compressedFileStreamPosition += bis.read(readBufferCompressed, 0, compressedLength);
                    decompressor.decompress(readBufferCompressed, 0, readBufferUncompressed, 0, uncompressedLength);
                    uncompressedFileStreamPosition += uncompressedLength;
                    datasetStartOffset = (int) (searchOffset - resultBufferStartOffset);
                    resultBuffer = concat(datasetStartOffset, readBufferUncompressed, resultBuffer, uncompressedLength);
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
        LZ4BlockInputStream lz4Is = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            lz4Is = new LZ4BlockInputStream(bis);
            br = new BufferedReader(new InputStreamReader(lz4Is, "UTF-8"));
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
            IOUtils.closeQuietly(lz4Is);
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

    @Override
    protected int getMagicHeaderSize() {
        return 0;
    }

    @Override
    protected int getBlockOverhead() {
        return 8;
    }
}