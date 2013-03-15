import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 3/14/13
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class SnappyTestOut {

    private static final int blockSize = 512 * 1024;
//    private static long current = 0l;

    public static void main(String[] args) throws Exception {
        FileOutputStream metaFos = new FileOutputStream("/Users/carsten/bla512k.meta.snappy");
        final DataOutputStream metaDos = new DataOutputStream(metaFos);

        FileOutputStream fos = new FileOutputStream("/Users/carsten/bla512k.snappy");
        BufferedOutputStream bos = new BufferedOutputStream(fos) {
            @Override
            public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                metaDos.writeInt(i2);
//                current += i2;
                super.write(bytes, i, i2);
            }
        };
        SnappyOutputStream sos = new SnappyOutputStream(bos, blockSize);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(sos));
        for(int i = 0; i < 500000000; i++) {
            writer.write("Hello World coooool kflsökfs kdfslj lgkdsfj  sfdkljkdsflö skfjg sdfj gkldsöj gös jo man " + i + " ...\n");
            if(i % 100000 == 0) {
                System.out.println("Nr: " + i);
            }
        }
        writer.close();
        sos.close();
        bos.close();
        fos.close();
        metaDos.close();
        metaFos.close();
    }
}
