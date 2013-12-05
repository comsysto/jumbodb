import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;

import java.security.MessageDigest;

/**
 * @author Carsten Hufe
 */
public class TestDigests {
    public static void main(String[] args) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] test = RandomStringUtils.randomAscii(32 * 1024).getBytes("UTF-8");//"Hello World test ksalg aflgsgfa".getBytes("UTF-8");
        long s = System.currentTimeMillis();
        for (int i = 0; i < (1024 * 1024); i++) {
            md.update(test);
        }
        md.digest();
        System.out.println(System.currentTimeMillis() - s);

    }
}
