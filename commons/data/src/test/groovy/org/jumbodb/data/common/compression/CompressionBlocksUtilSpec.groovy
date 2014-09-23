package org.jumbodb.data.common.compression

import org.apache.commons.lang.RandomStringUtils
import org.jumbodb.data.common.snappy.SnappyUtil
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class CompressionBlocksUtilSpec extends Specification {
    def "test getBlocksByFile"() {
        setup:
        def tempFile = File.createTempFile("test", "file")
        def bytes = RandomStringUtils.randomAlphabetic(128 * 1024).getBytes("UTF-8")
        def byteArrayInput = new ByteArrayInputStream(bytes)
        SnappyUtil.copy(byteArrayInput, tempFile, bytes.length, 100l, 32 * 1024)
        when:
        def blocks = CompressionBlocksUtil.getBlocksByFile(tempFile)
        then:
        blocks.getBlockSize() == 32 * 1024
        blocks.getBlocks().size() == 4
        blocks.getDatasets() == 100l
        blocks.getLength() == 128 * 1024
        blocks.getNumberOfBlocks() == 4
        blocks.getOffsetForBlock(3, 16, 4) == blocks.getBlocks()[0] + 4 + blocks.getBlocks()[1] + 4 + blocks.getBlocks()[2] + 4 + 16
        cleanup:
        tempFile.delete()
    }
}
