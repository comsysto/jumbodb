package org.jumbodb.database.service.query.snappy;

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class SnappyUtil {
    public static byte[] getUncompressed(RandomAccessFile indexRaf, SnappyChunks snappyChunks, long currentChunk) throws IOException {
        long offsetForChunk = snappyChunks.getOffsetForChunk(currentChunk);
        indexRaf.seek(offsetForChunk);
        int snappyBlockLength = indexRaf.readInt();
        byte[] compressed = new byte[snappyBlockLength];
        indexRaf.read(compressed);
        return Snappy.uncompress(compressed);
    }

    public static int readInt(byte[] buffer, int pos) {
        int b1 = (buffer[pos] & 0xFF) << 24;
        int b2 = (buffer[pos + 1] & 0xFF) << 16;
        int b3 = (buffer[pos + 2] & 0xFF) << 8;
        int b4 = buffer[pos + 3] & 0xFF;
        return b1 | b2 | b3 | b4;
    }

    public static long readLong(byte[] buffer, int pos) {
        long b1 = (buffer[pos] & 0xFF) << 56;
        long b2 = (buffer[pos + 1] & 0xFF) << 48;
        long b3 = (buffer[pos + 2] & 0xFF) << 40;
        long b4 = (buffer[pos + 3] & 0xFF) << 32;
        long b5 = (buffer[pos + 4] & 0xFF) << 24;
        long b6 = (buffer[pos + 5] & 0xFF) << 16;
        long b7 = (buffer[pos + 6] & 0xFF) << 8;
        long b8 = buffer[pos + 7] & 0xFF;
        return b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8;
    }
}
