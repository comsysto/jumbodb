package org.jumbodb.data.common.meta;

/**
 * Supported checksums
 */
public enum ChecksumType {

    NONE("none", "none"),
    MD5("MD5", ".md5"),
    SHA1("SHA-1", ".sha1");

    String digest;
    String fileSuffix;

    ChecksumType(String digest, String fileSuffix) {
        this.digest = digest;
        this.fileSuffix = fileSuffix;
    }

    public String getDigest() {
        return digest;
    }

    public String getFileSuffix() {
        return fileSuffix;
    }
}
