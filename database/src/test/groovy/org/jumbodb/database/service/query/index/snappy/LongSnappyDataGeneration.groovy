package org.jumbodb.database.service.query.index.snappy

import org.jumbodb.data.common.compression.CompressionBlocksUtil
import org.jumbodb.data.common.compression.CompressionUtil
import org.jumbodb.data.common.snappy.SnappyUtil
import org.jumbodb.database.service.query.index.common.BlockRange
import org.jumbodb.database.service.query.index.common.numeric.FileDataRetriever

/**
 * @author Carsten Hufe
 */
class LongSnappyDataGeneration {
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
        for(blocks in 1..11) {
            for(datasetInBlock in 1..1600) {
                dos.writeLong(i)
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
        SnappyUtil.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, 100l, blockSize)
        CompressionBlocksUtil.getBlocksByFile(file)
    }

    def static createFileDataRetriever(file, snappyChunks) {
        new FileDataRetriever() {

            @Override
            BlockRange<Long> getBlockRange(long searchChunk) throws IOException {
                def ramFile = new RandomAccessFile(file, "r")
                byte[] uncompressedBlock = SnappyUtil.getUncompressed(ramFile, snappyChunks, searchChunk)
                Long firstInt = CompressionUtil.readLong(uncompressedBlock, 0);
                Long lastInt = CompressionUtil.readLong(uncompressedBlock, uncompressedBlock.length - 20);
                ramFile.close()
                return new BlockRange<Long>(firstInt, lastInt);

            }
        }
    }
}