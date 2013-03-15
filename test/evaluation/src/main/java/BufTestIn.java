import org.xerial.snappy.SnappyInputStream;

import java.io.BufferedInputStream;
import java.io.FileInputStream;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 3/14/13
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class BufTestIn {
    public static void main(String[] args) throws Exception {
        FileInputStream fis = new FileInputStream("/Users/carsten/bla.buf");
        BufferedInputStream bis = new BufferedInputStream(fis);
        long start = System.currentTimeMillis();
        long l = 24l * 1024l * 1024l * 1024l;
        bis.skip(l);
        byte[] b = new byte[128];
        bis.read(b);
        System.out.println(new String(b));
        System.out.println("Time: " + (System.currentTimeMillis() - start));
        bis.close();
        bis.close();
        fis.close();
    }
}
