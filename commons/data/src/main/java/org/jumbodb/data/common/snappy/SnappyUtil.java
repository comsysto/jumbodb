package org.jumbodb.data.common.snappy;

import org.apache.commons.io.IOUtils;
import org.jumbodb.data.common.compression.Blocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

/**
 * Created by Carsten on 20.09.2014.
 */
public class SnappyUtil {
    private static Logger log = LoggerFactory.getLogger(SnappyUtil.class);

    public static byte[] getUncompressed(RandomAccessFile indexRaf, Blocks blocks, long searchChunk) throws IOException {
        long offsetForChunk = blocks.getOffsetForBlock(searchChunk);
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
     * @param chunkSize          snappy chunk size
     */
    // CARSTEN move copy method is only for tests
    public static void copy(InputStream inputStream, File absoluteImportFile, long fileLength, long datasets, int chunkSize) {
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

            snappyChunksDos.writeLong(fileLength);
            snappyChunksDos.writeLong(datasets);
            snappyChunksDos.writeInt(chunkSize);
            fos = new FileOutputStream(absoluteImportFile);
            bos = new BufferedOutputStream(fos) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    finalSnappyChunksDos.writeInt(i2);
                    super.write(bytes, i, i2);
                }
            };
            sos = new SnappyOutputStream(bos, chunkSize);
            IOUtils.copyLarge(inputStream, sos, 0l, fileLength);
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
