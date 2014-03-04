package org.jumbodb.data.common.snappy;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: carsten
 * Date: 3/25/13
 * Time: 3:16 PM
 */
public class SnappyChunksUtil {
    private static Logger log = LoggerFactory.getLogger(SnappyChunksUtil.class);

    /**
     * Copies stream to file
     *
     * @param dataInputStream
     * @param absoluteImportFile
     * @param fileLength
     * @param chunkSize
     * @return hash over uncompressed data
     */
    // CARSTEN delete
    public static String copy(InputStream dataInputStream, File absoluteImportFile, long fileLength, int chunkSize) {
        OutputStream sos = null;
        DataOutputStream dos = null;
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        FileOutputStream snappyChunksFos = null;
        DataOutputStream snappyChunksDos = null;
        DigestOutputStream md5DosRaw = null;
     //   DigestOutputStream sha1DosCompressed = null;
        MessageDigest md5DigestRaw = null;
     //   MessageDigest sha1DigestCompressed = null;
        try {
            md5DigestRaw = MessageDigest.getInstance("MD5");
     //       sha1DigestCompressed = MessageDigest.getInstance("SHA1");
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
            fos = new FileOutputStream(absoluteImportFile);
         //   sha1DosCompressed = new DigestOutputStream(fos, sha1DigestCompressed);
            bos = new BufferedOutputStream(fos) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    finalSnappyChunksDos.writeInt(i2);
                    super.write(bytes, i, i2);
                }
            };
            sos = new SnappyOutputStream(bos, chunkSize);
            md5DosRaw = new DigestOutputStream(sos, md5DigestRaw);
            IOUtils.copyLarge(dataInputStream, md5DosRaw, 0l, fileLength);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(md5DosRaw);
            IOUtils.closeQuietly(sos);
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
         //   IOUtils.closeQuietly(sha1DosCompressed);
            IOUtils.closeQuietly(fos);
            IOUtils.closeQuietly(snappyChunksDos);
            IOUtils.closeQuietly(snappyChunksFos);
        }

        // streams should be closed or flushed to get valid hashes!
        try {
/*            if(md5DigestRaw != null) {
                String sha1CompressHex = Hex.encodeHexString(sha1DigestCompressed.digest());
                FileUtils.write(new File(absoluteImportFile.getAbsolutePath() + ".sha1"), sha1CompressHex);
            }  */
            if(md5DigestRaw != null) {
                String md5DigestRawHex = Hex.encodeHexString(md5DigestRaw.digest());
                FileUtils.write(new File(absoluteImportFile.getAbsolutePath() + ".raw.md5"), md5DigestRawHex);
                return md5DigestRawHex;
            }
        } catch(IOException e) {
            throw new UnhandledException(e);
        }
        return "invalid_hash";
    }

    public static SnappyChunks getSnappyChunksByFile(File compressedFile) {
        String chunkFileName = compressedFile.getAbsolutePath() + ".chunks.snappy";
        File chunkFile = new File(chunkFileName);
        FileInputStream chunksFis = null;
        DataInputStream chunksDis = null;
        try {
            chunksFis = new FileInputStream(chunkFileName);
            chunksDis = new DataInputStream(new BufferedInputStream(chunksFis));
            long length = chunksDis.readLong();
            int snappyChunkSize = chunksDis.readInt();
            int numberOfChunks = (int)(chunkFile.length() - 8 - 4 - 4) / 4;
            List<Integer> snappyChunks = buildSnappyChunks(chunksDis, numberOfChunks);
            return new SnappyChunks(length, snappyChunkSize, numberOfChunks, snappyChunks);

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
        List<Integer> snappyChunks = new ArrayList<Integer>((int)numberOfChunks);
        // - 8 is length value, 4 is the chunksize
        chunksDis.readInt(); // remove version chunk
        for(int i = 0; i < numberOfChunks; i++) {
            snappyChunks.add(chunksDis.readInt());
        }
        return snappyChunks;
    }
}
