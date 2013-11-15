package org.jumbodb.connector.hadoop;

import com.sun.tools.javac.util.Paths;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.io.MD5Hash;

import java.security.DigestInputStream;
import java.security.MessageDigest;

/**
 * @author Carsten Hufe
 */
public class TestingMd5Hash {
    public static void main(String[] args) throws Exception {
        MD5Hash a = MD5Hash.digest("Hello World");
        System.out.println(a.toString());

        System.out.println(DigestUtils.md5Hex("Hello World"));

        MessageDigest complete = MessageDigest.getInstance("MD5");
        complete.update("Hello ".getBytes());
        complete.update("World1".getBytes());
        byte[] digest = complete.digest();
        char[] chars = Hex.encodeHex(digest);
        System.out.println(chars);
        System.out.println(Hex.encodeHex(Hex.decodeHex(chars)));

    }
}
