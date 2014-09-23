package org.jumbodb.database.service.query.index.lz4

import org.jumbodb.data.common.compression.CompressionBlocksUtil
import org.jumbodb.data.common.lz4.Lz4Util

/**
 * @author Carsten Hufe
 */
class FloatLz4DataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "idx")
    }

    def static createIndexContent() {
        def fos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(fos)

        // write 11 blocks

        def fileHash = 50000
        def offsetBase = 100000
        def i = -2048
        for (blocks in 1..11) {
            for (datasetInBlock in 1..2048) {
                dos.writeFloat(i)
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
        def blockSize = 32768
        def umcompressedFileLength = 16 * 11 * 2048 // index entry length * 12 blocks * datasets per chunk
        Lz4Util.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, 100l, blockSize)
        CompressionBlocksUtil.getBlocksByFile(file)
    }
}
