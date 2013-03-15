import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 3/14/13
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class BufferTestOut {
    public static void main(String[] args) throws Exception {
        FileOutputStream fos = new FileOutputStream("/Users/carsten/bla.buf");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(bos));
        for(int i = 0; i < 320000000; i++) {
            writer.write("Hello World coooool kflsökfs kdfslj lgkdsfj  sfdkljkdsflö skfjg sdfj gkldsöj gös jo man " + i + " ...\n");
            if(i % 100000 == 0) {
                System.out.println("Nr: " + i);
            }
        }
        writer.close();
        bos.close();
        fos.close();
    }
}
