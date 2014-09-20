package org.jumbodb.data.common.snappy

import org.apache.commons.io.IOUtils
import org.apache.commons.lang.RandomStringUtils
import org.jumbodb.data.common.compression.CompressionUtil
import org.xerial.snappy.SnappyOutputStream
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class ChunkSkipableSnappyInputStreamSpec extends Specification {

    def "skip to third block and read expected string"() {
        setup:
        def expectedString = "This is my expected string"
        def stringOverTwoBlocks = RandomStringUtils.randomAlphanumeric(64 * 1024) + expectedString
        def bos = new ByteArrayOutputStream()
        def sos = new SnappyOutputStream(bos, 32 * 1024)
        def bytes = stringOverTwoBlocks.getBytes("UTF-8")
        sos.write(bytes)
        sos.close()
        bos.close()
        def compressedData = bos.toByteArray()
        def compressedBlockSize1 = CompressionUtil.readInt(compressedData, 16) + 4
        def compressedBlockSize2 = CompressionUtil.readInt(compressedData, compressedBlockSize1 + 16) + 4
        when:
        def skipableStream = new ChunkSkipableSnappyInputStream(new ByteArrayInputStream(compressedData))
        then:
        skipableStream.getCurrentChunk() == 0
        when:
        skipableStream.skipCompressed(compressedBlockSize1 + compressedBlockSize2)
        then:
        IOUtils.toString(skipableStream) == expectedString
    }
}
