package org.jumbodb.database.service.query.index.doubleval.snappy

import org.apache.commons.lang.time.DateUtils
import org.jumbodb.data.common.snappy.SnappyChunksUtil

/**
 * @author Carsten Hufe
 */
class DoubleDataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "odx")
    }

    def static createIndexContent() {
        def fos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(fos)

        // write 11 chunks

        def fileHash = 50000
        def offsetBase = 100000
        def i = -1600
        for(chunks in 1..11) {
            for(datasetInChunk in 1..1600) {
                dos.writeDouble(i)
                dos.writeInt(fileHash)
                dos.writeLong(i + offsetBase)
                i++
            }
        }
        dos.close()
        fos.close()
        fos.toByteArray()
    }

    def static createIndexFile(file) {
        def chunkSize = 32000
        def umcompressedFileLength = 20 * 11 * 1600 // index entry length * 11 chunks * datasets per chunk
        SnappyChunksUtil.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, chunkSize)
        SnappyChunksUtil.getSnappyChunksByFile(file)
    }
}
