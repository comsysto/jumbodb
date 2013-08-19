package org.jumbodb.database.service.query.index.integer.snappy

import org.jumbodb.data.common.snappy.SnappyChunksUtil

/**
 * @author Carsten Hufe
 */
class IntegerDataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "odx")
    }

    def static createIndexContent() {
        def fos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(fos)

        // write 11 chunks

        def fileHash = 50000
        def offsetBase = 100000
        def i = -2048
        for(chunks in 1..11) {
            for(datasetInChunk in 1..2048) {
                dos.writeInt(i)
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
        def chunkSize = 32768
        def umcompressedFileLength = 16 * 11 * 2048 // index entry length * 12 chunks * datasets per chunk
        SnappyChunksUtil.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, chunkSize)
        SnappyChunksUtil.getSnappyChunksByFile(file)
    }
}
