package org.jumbodb.database.service.query.snappy;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * User: carsten
 * Date: 3/25/13
 * Time: 3:16 PM
 */
public class SnappyChunksUtil {
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
