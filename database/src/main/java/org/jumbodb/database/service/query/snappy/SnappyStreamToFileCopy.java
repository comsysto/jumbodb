package org.jumbodb.database.service.query.snappy;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

/**
 * @author Carsten Hufe
 */
public class SnappyStreamToFileCopy {
    private static Logger log = LoggerFactory.getLogger(SnappyStreamToFileCopy.class);

    public static void copy(InputStream dataInputStream, File absoluteImportFile, long fileLength, int chunkSize) {
        OutputStream sos = null;
        DataOutputStream dos = null;
        BufferedOutputStream bos = null;
        FileOutputStream snappyChunksFos = null;
        DataOutputStream snappyChunksDos = null;
        try {
            String absoluteImportPath = absoluteImportFile.getAbsolutePath() + "/";
            File storageFolderFile = new File(absoluteImportPath);
            if (!storageFolderFile.getParentFile().exists()) {
                if(!storageFolderFile.mkdirs()){
                    log.warn("Cannot create directory: " + storageFolderFile.getAbsolutePath());
                }
            }
            if (absoluteImportFile.exists()) {
                if(!absoluteImportFile.delete()){
                    log.warn("Cannot delete file: " + absoluteImportFile.getAbsolutePath());
                }
            }
            log.info("ImportServer - " + absoluteImportFile);

            String filePlaceChunksPath = absoluteImportFile.getAbsolutePath() + ".chunks.snappy";
            File filePlaceChunksFile = new File(filePlaceChunksPath);
            if (filePlaceChunksFile.exists()) {
                filePlaceChunksFile.delete();
            }
            snappyChunksFos = new FileOutputStream(filePlaceChunksFile);
            snappyChunksDos = new DataOutputStream(snappyChunksFos);
            final DataOutputStream finalSnappyChunksDos = snappyChunksDos;

            snappyChunksDos.writeLong(fileLength);
            snappyChunksDos.writeInt(chunkSize);
            // CARSTEN pfui, cleanup when time!
            bos = new BufferedOutputStream(new FileOutputStream(absoluteImportFile)) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    finalSnappyChunksDos.writeInt(i2);
                    super.write(bytes, i, i2);
                }
            };
            sos = new SnappyOutputStream(bos, chunkSize);
            IOUtils.copy(dataInputStream, sos);
            sos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
            IOUtils.closeQuietly(sos);
            IOUtils.closeQuietly(snappyChunksDos);
            IOUtils.closeQuietly(snappyChunksFos);
            IOUtils.closeQuietly(dataInputStream);
        }
    }
}
