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
        return (((long)buffer[pos] << 56) +
                ((long)(buffer[pos + 1] & 255) << 48) +
                ((long)(buffer[pos + 2] & 255) << 40) +
                ((long)(buffer[pos + 3] & 255) << 32) +
                ((long)(buffer[pos + 4] & 255) << 24) +
                ((buffer[pos + 5] & 255) << 16) +
                ((buffer[pos + 6] & 255) <<  8) +
                ((buffer[pos + 7] & 255) <<  0));
    }

    public static double readDouble(byte[] buffer, int pos) {
        long val = readLong(buffer, pos);
        return Double.longBitsToDouble(val);
    }

    public static float readFloat(byte[] buffer, int pos) {
        long val = readInt(buffer, pos);
        return Float.floatToIntBits(val);
    }
}
