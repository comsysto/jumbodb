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
        FileInputStream fis = new FileInputStream("/Users/carsten/smhadoop/output/twitter/2013-04-24-17-24-52/index/carsten.twitter/followers_count_long/part-r-00000.odx");
        BufferedInputStream bis = new BufferedInputStream(fis);
        DataInputStream dis = new DataInputStream(bis);

        int count = 0;
        while (dis.available() > 0) {
            byte buffer[] = new byte[8];
            dis.read(buffer);
            long value = readLong(buffer, 0);
//            long value = dis.readLong();
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
