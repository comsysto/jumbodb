import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 3/14/13
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class SnappyTestIn {
    public static void main(String[] args) throws Exception {
//        FileInputStream fis = new FileInputStream("/Users/carsten/jumbodb/index/carsten.twitter/first_delivery/4a7e1c02-3e69-418c-becb-4a68927850cc/followers_count/part-r-00000.odx");
//        BufferedInputStream bis = new BufferedInputStream(fis);
//        SnappyInputStream sis = new SnappyInputStream(bis);
//        DataInputStream dis = new DataInputStream(sis);
//
//        for (int i = 0; i < 3000; i++) {
//            System.out.println(dis.readInt());
//            dis.readInt();
//            dis.readLong();
//        }
//        dis.close();
//        sis.close();
//        bis.close();
//        fis.close();

//        FileInputStream fis = new FileInputStream("/Users/carsten/smhadoop/output/twitter/2013-04-24-10-00-16/index/carsten.twitter/followers_count/part-r-00000.odx");
//        BufferedInputStream bis = new BufferedInputStream(fis);
//        DataInputStream dis = new DataInputStream(bis);
//
//        int count = 0;
//        while (dis.available() > 0) {
//            int value = dis.readInt();
//            if(value < 100000) {
////            if(value > 150 && value < 200) {
//                count++;
//                System.out.println(value);
//            }
//            dis.readInt();
//            dis.readLong();
//        }
//        System.out.println("count " + count);
//        dis.close();
//        bis.close();
//        fis.close();
        FileInputStream fis = new FileInputStream("/Users/carsten/smhadoop/output/twitter/2013-04-24-18-09-54/index/carsten.twitter/followers_count_double/part-r-00000.odx");
        BufferedInputStream bis = new BufferedInputStream(fis);
        DataInputStream dis = new DataInputStream(bis);

        int count = 0;
        while (dis.available() > 0) {
            byte buffer[] = new byte[8];
            dis.read(buffer);
            double value = readDouble(buffer, 0);
//            Double value = dis.readDouble();
//            if(value < 100000) {
//            if(value > 150 && value < 200) {
                count++;
                System.out.println(value);
//            }
            dis.readInt();
            dis.readLong();
        }
        System.out.println("count " + count);
        dis.close();
        bis.close();
        fis.close();
    }


    public static double readDouble(byte[] buffer, int pos) {
        long val = readLong(buffer, pos);
        return Double.longBitsToDouble(val);
    }

    public static float readFloat(byte[] buffer, int pos) {
        long val = readInt(buffer, pos);
        return Float.floatToIntBits(val);
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
