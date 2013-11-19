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
     * @return SHA-1 hash over uncompressed data
     */
    public static String copy(InputStream dataInputStream, File absoluteImportFile, long fileLength, int chunkSize) {
        OutputStream sos = null;
        DataOutputStream dos = null;
        FileOutputStream fos = null;
        BufferedOutputStream bos = null;
        FileOutputStream snappyChunksFos = null;
        DataOutputStream snappyChunksDos = null;
        DigestOutputStream sha1DosRaw = null;
        DigestOutputStream sha1DosCompressed = null;
        MessageDigest sha1DigestRaw = null;
        MessageDigest sha1DigestCompressed = null;
        try {
            sha1DigestRaw = MessageDigest.getInstance("SHA1");
            sha1DigestCompressed = MessageDigest.getInstance("SHA1");
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
            sha1DosCompressed = new DigestOutputStream(fos, sha1DigestCompressed);
            bos = new BufferedOutputStream(sha1DosCompressed) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    finalSnappyChunksDos.writeInt(i2);
                    super.write(bytes, i, i2);
                }
            };
            sos = new SnappyOutputStream(bos, chunkSize);
            sha1DosRaw = new DigestOutputStream(sos, sha1DigestRaw);
            IOUtils.copyLarge(dataInputStream, sha1DosRaw, 0l, fileLength);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(sha1DosRaw);
            IOUtils.closeQuietly(sos);
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
            IOUtils.closeQuietly(sha1DosCompressed);
            IOUtils.closeQuietly(fos);
            IOUtils.closeQuietly(snappyChunksDos);
            IOUtils.closeQuietly(snappyChunksFos);
        }

        // streams should be closed or flushed to get valid hashes!
        try {
            if(sha1DigestRaw != null) {
                String sha1CompressHex = Hex.encodeHexString(sha1DigestCompressed.digest());
                FileUtils.write(new File(absoluteImportFile.getAbsolutePath() + ".sha1"), sha1CompressHex);
            }
            if(sha1DigestCompressed != null) {
                String sha1DigestRawHex = Hex.encodeHexString(sha1DigestRaw.digest());
                FileUtils.write(new File(absoluteImportFile.getAbsolutePath() + ".raw.sha1"), sha1DigestRawHex);
                return sha1DigestRawHex;
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
