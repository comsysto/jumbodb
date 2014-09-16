package org.jumbodb.data.common.snappy;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class SnappyChunksUtil {
    private static Logger log = LoggerFactory.getLogger(SnappyChunksUtil.class);
    private static final long CHUNK_HEADER_SIZE = 8 + 8 + 4 + 4;
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

            String filePlaceChunksPath = absoluteImportFile.getAbsolutePath() + ".chunks";
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

    public static SnappyChunks getSnappyChunksByFile(File compressedFile) {
        String chunkFileName = compressedFile.getAbsolutePath() + ".chunks";
        File chunkFile = new File(chunkFileName);
        FileInputStream chunksFis = null;
        DataInputStream chunksDis = null;
        try {
            chunksFis = new FileInputStream(chunkFileName);
            chunksDis = new DataInputStream(new BufferedInputStream(chunksFis));
            long length = chunksDis.readLong();
            long datasets = chunksDis.readLong();
            int snappyChunkSize = chunksDis.readInt();
            int numberOfChunks = (int) (chunkFile.length() - CHUNK_HEADER_SIZE) / 4;
            List<Integer> snappyChunks = buildSnappyChunks(chunksDis, numberOfChunks);
            return new SnappyChunks(length, datasets, snappyChunkSize, numberOfChunks, snappyChunks);

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(chunksDis);
            IOUtils.closeQuietly(chunksFis);
        }
    }

    private static List<Integer> buildSnappyChunks(DataInputStream chunksDis, long numberOfChunks) throws IOException {
        List<Integer> snappyChunks = new ArrayList<Integer>((int) numberOfChunks);
        // - 8 is length value, 4 is the chunksize
        chunksDis.readInt(); // remove version chunk
        for (int i = 0; i < numberOfChunks; i++) {
            snappyChunks.add(chunksDis.readInt());
        }
        return snappyChunks;
    }
}
