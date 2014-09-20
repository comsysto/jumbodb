package org.jumbodb.database.service.query.data.lz4;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.Utils;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.data.common.compression.Blocks;
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
import java.util.zip.Checksum;


public class JsonLz4RetrieveDataSetsTask extends DefaultRetrieveDataSetsTask {

    private ObjectMapper jsonParser = new ObjectMapper();

    public JsonLz4RetrieveDataSetsTask(File file, Set<FileOffset> offsets, JumboQuery searchQuery,
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
//            byte[] readBufferCompressed = new byte[blocks.getBlockSize() * 2];
//            byte[] readBufferUncompressed = new byte[blocks.getBlockSize() * 2];
            byte[] resultBuffer = EMPTY_BUFFER;
            long resultBufferStartOffset = 0l;
            long resultBufferEndOffset = 0l;
            byte[] compressedLengthBuffer = new byte[4];
            byte[] uncompressedLengthBuffer = new byte[4];
            byte[] checksumBuffer = new byte[4];
            long uncompressedFileStreamPosition = 0l;
            long compressedFileStreamPosition = 0l;
            final Checksum checksum = XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c).asChecksum();
            LZ4Factory factory = LZ4Factory.fastestInstance();
            LZ4FastDecompressor decompressor = factory.fastDecompressor();
            for (FileOffset offset : leftOffsets) {
                long searchOffset = offset.getOffset();
                // delete buffer when offset is not inside range and skip
                if (resultBuffer.length == 0 || (resultBufferStartOffset < searchOffset && searchOffset > resultBufferEndOffset)) {
                    long blockIndex = (searchOffset / blocks.getBlockSize());
                    long blockOffsetCompressed = calculateBlockOffsetCompressed(blockIndex, blocks.getBlocks());
                    long blockOffsetUncompressed = calculateBlockOffsetUncompressed(blockIndex,
                            blocks.getBlockSize());
                    long blockOffsetToSkip = blockOffsetCompressed - compressedFileStreamPosition;
                    long skip = skipToOffset(bis, blockOffsetToSkip);
                    compressedFileStreamPosition += skip;
                    uncompressedFileStreamPosition = blockOffsetUncompressed;
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
//                    byte[] magic = new byte[8];
//                    compressedFileStreamPosition += bis.read(magic); // magic header and token
//                    String x = new String(magic);
//                    System.out.println(x);
                    compressedFileStreamPosition += bis.skip(9); // magic header and token
                    compressedFileStreamPosition += bis.read(compressedLengthBuffer);
                    compressedFileStreamPosition += bis.read(uncompressedLengthBuffer);
                    compressedFileStreamPosition += bis.read(checksumBuffer);

                    int compressedLength = Utils.readIntLE(compressedLengthBuffer, 0);
                    int uncompressedLength = Utils.readIntLE(uncompressedLengthBuffer, 0);
                    int checksumBlock = Utils.readIntLE(checksumBuffer, 0);

                    byte[] readBufferCompressed = new byte[compressedLength];
                    byte[] readBufferUncompressed = new byte[uncompressedLength];
                    int read = bis.read(readBufferCompressed, 0, compressedLength);
                    compressedFileStreamPosition += read;
                    decompressor.decompress(readBufferCompressed, readBufferUncompressed);
                    checksum.reset();
                    checksum.update(readBufferUncompressed, 0, uncompressedLength);
                    if ((int) checksum.getValue() != checksumBlock) {
                        throw new IOException("Stream is corrupted");
                    }
//                    int uncompressLength = Snappy
//                            .uncompress(readBufferCompressed, 0, compressedLength, readBufferUncompressed, 0);
                    uncompressedFileStreamPosition += uncompressedLength;
                    datasetStartOffset = (int) (searchOffset - resultBufferStartOffset);
                    resultBuffer = concat(datasetStartOffset, readBufferUncompressed, resultBuffer, uncompressedLength);
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

    @Override
    protected int getMagicHeaderSize() {
        // header is starting in each block...
        return 0;
    }

    @Override
    protected int getBlockOverhead() {
        return 21;
    }
}