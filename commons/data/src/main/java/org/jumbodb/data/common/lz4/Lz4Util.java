package org.jumbodb.data.common.lz4;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.Utils;
import org.apache.commons.io.IOUtils;
import org.jumbodb.data.common.compression.Blocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Carsten on 20.09.2014.
 */
// CARSTEN unit test
public class Lz4Util {
    public static final int HEADER_SIZE = 0;
    public static final int BLOCK_OVERHEAD = 8;

    private static Logger log = LoggerFactory.getLogger(Lz4Util.class);
    private static LZ4Factory factory = LZ4Factory.fastestInstance();
    private static LZ4FastDecompressor decompressor = factory.fastDecompressor();

    public static byte[] getUncompressed(RandomAccessFile indexRaf, Blocks blocks, long blockIndex) throws IOException {
        long offsetForChunk = blocks.getOffsetForBlock(blockIndex, HEADER_SIZE, BLOCK_OVERHEAD);
        indexRaf.seek(offsetForChunk);
        byte[] lengthsBuffer = new byte[8];
        indexRaf.read(lengthsBuffer);
        int compressedLength = Utils.readIntLE(lengthsBuffer, 0);
        int uncompressedLength = Utils.readIntLE(lengthsBuffer, 4);
        byte[] compressed = new byte[compressedLength];
        indexRaf.read(compressed);
        return decompressor.decompress(compressed, uncompressedLength);
    }

    /**
     * Copies stream to file
     *
     * @param inputStream        input stream write to disk
     * @param absoluteImportFile path where to write
     * @param fileLength         length of data
     * @param blockSize          snappy chunk size
     */
    public static void copy(InputStream inputStream, File absoluteImportFile, long fileLength, long datasets, int blockSize) {
        OutputStream lz4os = null;
        DataOutputStream dos = null;
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        FileOutputStream lz4BlockFos = null;
        DataOutputStream lz4BlockDos = null;
        try {
            String absoluteImportPath = absoluteImportFile.getAbsolutePath() + "/";
            File storageFolderFile = new File(absoluteImportPath);
            if (!storageFolderFile.getParentFile().exists()) {
                if (!storageFolderFile.mkdirs()) {
                    log.warn("Cannot create directory: " + storageFolderFile.getAbsolutePath());
                }
            }
            if (absoluteImportFile.exists()) {
                if (!absoluteImportFile.delete()) {
                    log.warn("Cannot delete file: " + absoluteImportFile.getAbsolutePath());
                }
            }

            String filePlaceBlocksPath = absoluteImportFile.getAbsolutePath() + ".blocks";
            File filePlaceBlocksFile = new File(filePlaceBlocksPath);
            if (filePlaceBlocksFile.exists()) {
                filePlaceBlocksFile.delete();
            }
            lz4BlockFos = new FileOutputStream(filePlaceBlocksFile);
            lz4BlockDos = new DataOutputStream(lz4BlockFos);
            final DataOutputStream finalLz4BlocksDos = lz4BlockDos;


            final List<Integer> blocks = new LinkedList<Integer>();
            fos = new FileOutputStream(absoluteImportFile);
            bos = new BufferedOutputStream(fos);
            lz4os = new LZ4BlockOutputStream(bos, blockSize) {
                @Override
                protected void onCompressedLength(int compressedLength) {
                    blocks.add(compressedLength);
                }
            };
            IOUtils.copyLarge(inputStream, lz4os, 0l, fileLength);
            lz4os.flush();
            lz4BlockDos.writeLong(fileLength);
            lz4BlockDos.writeLong(datasets);
            lz4BlockDos.writeInt(blockSize);
            lz4BlockDos.writeInt(blocks.size());
            for (Integer block : blocks) {
                finalLz4BlocksDos.writeInt(block);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(lz4os);
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
            IOUtils.closeQuietly(fos);
            IOUtils.closeQuietly(lz4BlockDos);
            IOUtils.closeQuietly(lz4BlockFos);
        }
    }
}
