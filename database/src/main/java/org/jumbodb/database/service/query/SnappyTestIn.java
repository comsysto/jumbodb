package org.jumbodb.database.service.query;

import org.apache.commons.io.IOUtils;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyChunksUtil;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 3/14/13
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class SnappyTestIn {
    protected static long calculateChunkOffsetUncompressed(long chunkIndex, int snappyChunkSize) {
        return chunkIndex * snappyChunkSize;
    }

    protected static long calculateChunkOffsetCompressed(long chunkIndex, List<Integer> snappyChunks) {
        long result = 0l;
        for(int i = 0; i < chunkIndex; i++) {
            result += snappyChunks.get(i) + 4; // 4 byte for length of chunk
        }
        return result + 16;
    }

    public static void main(String[] args) throws Exception {
        String s = "/Users/carsten/testen/part-r-00009";
        File file = new File(s);
        SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(file);
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis);
        DataInputStream dis = new DataInputStream(fis);

        long compressedFileStreamPosition = 0l;
//
        long searchOffset = 16148062;
        long chunkIndex = (searchOffset / snappyChunks.getChunkSize());
        long chunkOffsetCompressed = calculateChunkOffsetCompressed(chunkIndex, snappyChunks.getChunks());
        long chunkOffsetUncompressed = calculateChunkOffsetUncompressed(chunkIndex, snappyChunks.getChunkSize());
        long chunkOffsetToSkip = chunkOffsetCompressed - compressedFileStreamPosition;

        compressedFileStreamPosition += dis.skip(chunkOffsetCompressed);
//        int i2 = dis.readInt();
//        System.out.println(i2);
//        byte[] buffer2 = new byte[i2];
//        filePosition += dis.read(buffer2) + 4;
//
//
//        searchOffset = 16148062;
//        chunkIndex = (searchOffset / snappyChunks.getChunkSize());
//        chunkOffsetCompressed = calculateChunkOffsetCompressed(chunkIndex, snappyChunks.getChunks());
//        dis.skip(chunkOffsetCompressed - filePosition);

//        dis.skip(2674046);
        int i = dis.readInt();
        System.out.println(i);
        byte[] buffer = new byte[i];
        dis.read(buffer);
        System.out.println(new String(Snappy.uncompress(buffer)));
//        dis.skip(16);
//        while(dis.available() > 0) {
//            int i = dis.readInt();
//            byte[] buffer = new byte[i];
//            dis.read(buffer);
//        }

        IOUtils.closeQuietly(dis);
        IOUtils.closeQuietly(bis);
        IOUtils.closeQuietly(fis);

//
//
//    public static double readDouble(byte[] buffer, int pos) {
//        long val = readLong(buffer, pos);
//        return Double.longBitsToDouble(val);
//    }
//
//    public static float readFloat(byte[] buffer, int pos) {
//        long val = readInt(buffer, pos);
//        return Float.floatToIntBits(val);
//    }
//
//    public static int readInt(byte[] buffer, int pos) {
//        int b1 = (buffer[pos] & 0xFF) << 24;
//        int b2 = (buffer[pos + 1] & 0xFF) << 16;
//        int b3 = (buffer[pos + 2] & 0xFF) << 8;
//        int b4 = buffer[pos + 3] & 0xFF;
//        return b1 | b2 | b3 | b4;
//    }
//
//    public static long readLong(byte[] buffer, int pos) {
//        return (((long)buffer[pos] << 56) +
//                ((long)(buffer[pos + 1] & 255) << 48) +
//                ((long)(buffer[pos + 2] & 255) << 40) +
//                ((long)(buffer[pos + 3] & 255) << 32) +
//                ((long)(buffer[pos + 4] & 255) << 24) +
//                ((buffer[pos + 5] & 255) << 16) +
//                ((buffer[pos + 6] & 255) <<  8) +
//                ((buffer[pos + 7] & 255) <<  0));
//    }
//        long b1 = (buffer[pos] & 0xFF) << 56;
//        long b2 = (buffer[pos + 1] & 0xFF) << 48;
//        long b3 = (buffer[pos + 2] & 0xFF) << 40;
//        long b4 = (buffer[pos + 3] & 0xFF) << 32;
//        long b5 = (buffer[pos + 4] & 0xFF) << 24;
//        long b6 = (buffer[pos + 5] & 0xFF) << 16;
//        long b7 = (buffer[pos + 6] & 0xFF) << 8;
//        long b8 = buffer[pos + 7] & 0xFF;
//        return b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8;
    }
}

