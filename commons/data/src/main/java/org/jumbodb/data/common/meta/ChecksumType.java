package org.jumbodb.data.common.meta;

/**
 * Supported checksums
 */
public enum ChecksumType {

    NONE("none"),
    MD5("MD5"),
    SHA1("SHA-1");

    String digest;

    ChecksumType(final String digest) {
        this.digest = digest;
    }

    public String getDigest() {
        return digest;
    }
}
