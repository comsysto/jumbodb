package org.jumbodb.database.service.query.index.lz4

import org.jumbodb.data.common.compression.CompressionBlocksUtil
import org.jumbodb.data.common.lz4.Lz4Util

/**
 * @author Carsten Hufe
 */
class DoubleLz4DataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "idx")
    }

    def static createIndexContent() {
        def fos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(fos)

        // write 11 blocks

        def fileHash = 50000
        def offsetBase = 100000
        def i = -1600
        for (blocks in 1..11) {
            for (datasetInBlock in 1..1600) {
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
        def blockSize = 32000
        def umcompressedFileLength = 20 * 11 * 1600 // index entry length * 11 blocks * datasets per chunk
        Lz4Util.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, 100l, blockSize)
        CompressionBlocksUtil.getBlocksByFile(file)
    }
}
