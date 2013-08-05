package org.jumbodb.database.service.query.index.datetime.snappy

import org.apache.commons.lang.time.DateUtils
import org.jumbodb.database.service.query.snappy.SnappyChunks
import org.jumbodb.database.service.query.snappy.SnappyChunksUtil
import org.jumbodb.database.service.query.snappy.SnappyStreamToFileCopy
import org.xerial.snappy.SnappyOutputStream

/**
 * @author Carsten Hufe
 */
class DateTimeDataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "odx")
    }

    def static createIndexContent() {
        def fos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(fos)

        // write 12 blocks, one per month

        def startDate = Date.parse("yyyy-MM-dd HH:mm:ss", "2012-01-01 12:00:00")
        def fileHash = 50000
        def offsetBase = 100000
        def i = 0
        for(month in 0..11) {
            def date = DateUtils.setMonths(startDate, month)
            for(dataset in 1..1600) {
                dos.writeLong(date.getTime())
                dos.writeInt(fileHash)
                dos.writeLong(date.getTime() + offsetBase)
                date = DateUtils.addSeconds(date, 1512) // 1512 = (28 days * 24 h * 60 min * 60s) / 1600
                i++
            }
        }
        dos.close()
        fos.close()
        fos.toByteArray()
    }

    def static createIndexFile(file) {
        def chunkSize = 32000
        def umcompressedFileLength = 20 * 12 * 1600 // index entry length * 12 months * datasets per month
        SnappyStreamToFileCopy.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, chunkSize)
        SnappyChunksUtil.getSnappyChunksByFile(file)
    }
}
