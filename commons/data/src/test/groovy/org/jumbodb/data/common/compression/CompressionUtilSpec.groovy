package org.jumbodb.data.common.compression

import org.apache.commons.lang.RandomStringUtils
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class CompressionUtilSpec extends Specification {
    def "test getUncompressed"() {
        setup:
        def string = RandomStringUtils.randomAlphanumeric(32 * 1024) + "This is my expected string"
        def byteStream = new ByteArrayInputStream(string.getBytes("UTF-8"))
        def testFile = File.createTempFile("test", "file")
        CompressionBlocksUtil.copy(byteStream, testFile, string.size(), 100l, 32 * 1024)
        def chunks = CompressionBlocksUtil.getBlocksByFile(testFile)
        byteStream.close()
        def raf = new RandomAccessFile(testFile, "r")
        when:
        def result = CompressionUtil.getUncompressed(raf, chunks, 1)
        then:
        new String(result, "UTF-8") == "This is my expected string"
        cleanup:
        raf.close()
        testFile.delete()
    }

    def "test readInt"() {
        setup:
        def byteStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteStream)
        dos.writeInt(111111)
        dos.writeInt(222222)
        dos.writeInt(333333)
        dos.writeInt(444444)
        dos.writeInt(555555)
        dos.close()
        byteStream.close()
        def bytes = byteStream.toByteArray()
        when:
        def readValue = CompressionUtil.readInt(bytes, 8)
        then:
        readValue == 333333
    }

    def "test readLong"() {
        setup:
        def byteStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteStream)
        dos.writeLong(111111l)
        dos.writeLong(222222l)
        dos.writeLong(333333l)
        dos.writeLong(444444l)
        dos.writeLong(555555l)
        dos.close()
        byteStream.close()
        def bytes = byteStream.toByteArray()
        when:
        def readValue = CompressionUtil.readLong(bytes, 24)
        then:
        readValue == 444444l
    }

    def "test readDouble"() {
        setup:
        def byteStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteStream)
        dos.writeDouble(111111.1d)
        dos.writeDouble(222222.2d)
        dos.writeDouble(333333.3d)
        dos.writeDouble(444444.4d)
        dos.writeDouble(555555.5d)
        dos.close()
        byteStream.close()
        def bytes = byteStream.toByteArray()
        when:
        def readValue = CompressionUtil.readDouble(bytes, 24)
        then:
        readValue == 444444.4d
    }

    def "test readFloat"() {
        setup:
        def byteStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteStream)
        dos.writeFloat(111111.1f)
        dos.writeFloat(222222.2f)
        dos.writeFloat(333333.3f)
        dos.writeFloat(444444.4f)
        dos.writeFloat(555555.5f)
        dos.close()
        byteStream.close()
        def bytes = byteStream.toByteArray()
        when:
        def readValue = CompressionUtil.readFloat(bytes, 16)
        then:
        readValue == 555555.5f
    }
}
