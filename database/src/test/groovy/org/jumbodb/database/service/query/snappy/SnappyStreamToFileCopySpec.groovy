package org.jumbodb.database.service.query.snappy

import org.jumbodb.data.common.snappy.SnappyStreamToFileCopy
import org.xerial.snappy.SnappyInputStream

/**
 * @author Carsten Hufe
 */
class SnappyStreamToFileCopySpec extends spock.lang.Specification {
    def "copy stream should be snappy compressed saved"() {
        setup:
        def strToCompare = "Some content for the file"
        def strToWrite = strToCompare + "\n"
        def byteArrayInputStream = new ByteArrayInputStream("Some content for the file".getBytes())
        def tmpTargetFile = File.createTempFile("outputFile", "snappy")
        when:
        SnappyStreamToFileCopy.copy(byteArrayInputStream, tmpTargetFile, strToWrite.size(), 32 * 1024)
        def is = new SnappyInputStream(new FileInputStream(tmpTargetFile))
        def reader = new BufferedReader(new InputStreamReader(is))
        then:
        reader.readLine() == strToCompare
        cleanup:
        is.close()
        reader.close()
        tmpTargetFile.delete()
    }
}
