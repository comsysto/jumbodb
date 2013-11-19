package org.jumbodb.connector.hadoop;

import org.apache.commons.codec.binary.Hex;
import java.security.MessageDigest;

/**
 * @author Carsten Hufe
 */
public class TestingMd5Hash {
    public static void main(String[] args) throws Exception {

        MessageDigest complete = MessageDigest.getInstance("SHA-1");
        System.out.println(Hex.encodeHexString(complete.digest()));
        complete.update("Hello ".getBytes());
        complete.update("World1".getBytes());
        System.out.println(Hex.encodeHexString(complete.digest()));

    }
}
