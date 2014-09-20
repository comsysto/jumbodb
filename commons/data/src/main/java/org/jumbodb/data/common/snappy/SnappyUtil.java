package org.jumbodb.data.common.snappy;

import org.apache.commons.io.IOUtils;
import org.jumbodb.data.common.compression.Blocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Carsten on 20.09.2014.
 */
public class SnappyUtil {
    private static Logger log = LoggerFactory.getLogger(SnappyUtil.class);

    public static byte[] getUncompressed(RandomAccessFile indexRaf, Blocks blocks, long blockIndex) throws IOException {
        long offsetForChunk = blocks.getOffsetForBlock(blockIndex);
        indexRaf.seek(offsetForChunk);
        int snappyBlockLength = indexRaf.readInt();
        byte[] compressed = new byte[snappyBlockLength];
        indexRaf.read(compressed);
        return Snappy.uncompress(compressed);
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
        OutputStream sos = null;
        DataOutputStream dos = null;
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        FileOutputStream snappyChunksFos = null;
        DataOutputStream snappyChunksDos = null;
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

            String filePlaceChunksPath = absoluteImportFile.getAbsolutePath() + ".blocks";
            File filePlaceChunksFile = new File(filePlaceChunksPath);
            if (filePlaceChunksFile.exists()) {
                filePlaceChunksFile.delete();
            }
            snappyChunksFos = new FileOutputStream(filePlaceChunksFile);
            snappyChunksDos = new DataOutputStream(snappyChunksFos);
            final DataOutputStream finalSnappyChunksDos = snappyChunksDos;
            final List<Integer> blocks = new ArrayList<Integer>();
            fos = new FileOutputStream(absoluteImportFile);
            bos = new BufferedOutputStream(fos) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    blocks.add(i2);
                    super.write(bytes, i, i2);
                }
            };
            sos = new SnappyOutputStream(bos, blockSize);
            IOUtils.copyLarge(inputStream, sos, 0l, fileLength);
            snappyChunksDos.writeLong(fileLength);
            snappyChunksDos.writeLong(datasets);
            snappyChunksDos.writeInt(blockSize);
            snappyChunksDos.writeInt(blocks.size() - 1);
            // remove magic header, so start at 1
            for(int i = 1; i < blocks.size(); i++) {
                finalSnappyChunksDos.writeInt(blocks.get(i));
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(sos);
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
            IOUtils.closeQuietly(fos);
            IOUtils.closeQuietly(snappyChunksDos);
            IOUtils.closeQuietly(snappyChunksFos);
        }
    }
}
