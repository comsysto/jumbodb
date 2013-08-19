package org.jumbodb.data.common.snappy

import org.apache.commons.lang.RandomStringUtils
import org.xerial.snappy.SnappyOutputStream
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class SnappyChunksUtilSpec extends Specification {
    def "test getSnappyChunksByFile"() {
        setup:
        def tempFile = File.createTempFile("test", "file")
        def byteArrayInput = new ByteArrayInputStream(RandomStringUtils.randomAlphabetic(128 * 1024).getBytes("UTF-8"))
        SnappyChunksUtil.copy(byteArrayInput, tempFile, 128 * 1024, 32 * 1024)
        when:
        def chunks = SnappyChunksUtil.getSnappyChunksByFile(tempFile)
        then:
        chunks.getChunkSize() == 32 * 1024
        chunks.getChunks().size() == 4
        chunks.getLength() == 128 * 1024
        chunks.getNumberOfChunks() == 4
        chunks.getOffsetForChunk(3) == chunks.getChunks()[0] + 4 + chunks.getChunks()[1] + 4 + chunks.getChunks()[3] + 4 + 16
        cleanup:
        tempFile.delete()
    }
}
