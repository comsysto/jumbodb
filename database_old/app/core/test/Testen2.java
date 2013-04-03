package core.test;

import org.xerial.snappy.SnappyInputStream;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 1/10/13
 * Time: 9:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class Testen2 {
    public static void main(String[] args) throws Exception {
//        LZFFileInputStream fis = new LZFFileInputStream("/Users/carsten/test.lzf");
        FileInputStream fis = new FileInputStream("/Users/carsten/test.snappy");
        long start = System.currentTimeMillis();
        System.out.println(fis.available());

//        GZIPInputStream s = new GZIPInputStream(fis);
        BufferedInputStream b = new BufferedInputStream(fis);
        SnappyInputStream s = new SnappyInputStream(b);
        b.skip(1100000000);
        b.read(new byte[1024 * 32]);
        System.out.println(s.available());
        // carsten wie lang dauert der skip ohne zeile lesen?
//        System.out.println(b.readLine());
        fis.close();
        System.out.println("Time " +  (System.currentTimeMillis() - start));
    }
}
