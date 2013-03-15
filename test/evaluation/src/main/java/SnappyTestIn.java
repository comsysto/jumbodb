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
        FileInputStream fis = new FileInputStream("/Users/carsten/bla512k.snappy");
        BufferedInputStream bis = new BufferedInputStream(fis);
        SnappyInputStream sis = new SnappyInputStream(bis);
        long start = System.currentTimeMillis();
        long l = 32l * 1024l * 1024l * 1024l;
        sis.skip(l);
        byte[] b = new byte[128];
        sis.read(b);
        System.out.println(new String(b));
        System.out.println("Time: " + (System.currentTimeMillis() - start));
        sis.close();
        bis.close();
        fis.close();
    }
}
