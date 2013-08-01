package org.jumbodb.database.service.query.index.datetime.snappy

import org.apache.commons.lang.time.DateUtils
import org.jumbodb.database.service.query.snappy.SnappyChunks
import org.xerial.snappy.SnappyOutputStream

/**
 * @author Carsten Hufe
 */
class DateTimeDataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "odx")
    }

    def static createIndexFile(file) {
        def chunks = new LinkedList<Integer>()
        def fos = new FileOutputStream(file)  {
            @Override
            public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                chunks.add(i2);
                super.write(bytes, i, i2);
            }
        }
        def chunkSize = 32000
        def sos = new SnappyOutputStream(fos, chunkSize)
        def dos = new DataOutputStream(sos)

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
        sos.close()
        fos.close()

        def umcompressedFileLength = 20 * 12 * 1600 // index entry length * 12 months * datasets per month
        chunks.remove(0) // snappy meta data
        new SnappyChunks(umcompressedFileLength, chunkSize, 12, chunks)
    }
}
