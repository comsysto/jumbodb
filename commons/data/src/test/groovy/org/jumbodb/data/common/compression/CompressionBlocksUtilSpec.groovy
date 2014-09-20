package org.jumbodb.data.common.compression

import org.apache.commons.lang.RandomStringUtils
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class CompressionBlocksUtilSpec extends Specification {
    def "test getSnappyChunksByFile"() {
        setup:
        def tempFile = File.createTempFile("test", "file")
        def bytes = RandomStringUtils.randomAlphabetic(128 * 1024).getBytes("UTF-8")
        def byteArrayInput = new ByteArrayInputStream(bytes)
        CompressionBlocksUtil.copy(byteArrayInput, tempFile, bytes.length, 100l, 32 * 1024)
        when:
        def chunks = CompressionBlocksUtil.getBlocksByFile(tempFile)
        then:
        chunks.getBlockSize() == 32 * 1024
        chunks.getBlocks().size() == 4
        chunks.getDatasets() == 100l
        chunks.getLength() == 128 * 1024
        chunks.getNumberOfBlocks() == 4
        chunks.getOffsetForBlock(3) == chunks.getBlocks()[0] + 4 + chunks.getBlocks()[1] + 4 + chunks.getBlocks()[2] + 4 + 16
        cleanup:
        tempFile.delete()
    }
}
