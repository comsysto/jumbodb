import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.lang.RandomStringUtils;
import org.xerial.snappy.Snappy;

import java.util.zip.Checksum;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 3/14/13
 * Time: 3:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class Other {
    public static void main(String[] args) {
        StreamingXXHash32 streamingXXHash32 = XXHashFactory.fastestInstance().newStreamingHash32(0x9747b28c);
        Checksum checksum = streamingXXHash32.asChecksum();
        byte[] s = RandomStringUtils.randomAlphanumeric(1024 * 1024).getBytes();
        long start = System.currentTimeMillis();
        for (int i = 0; i < (100 * 1024); i++) {
//            checksum.update(s, 0, s.length);
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
