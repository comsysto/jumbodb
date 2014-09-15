package org.jumbodb.database.service.query.index.common.hashcode64

import org.jumbodb.data.common.snappy.SnappyChunksUtil
import org.jumbodb.data.common.snappy.SnappyUtil
import org.jumbodb.database.service.query.index.common.BlockRange
import org.jumbodb.database.service.query.index.common.numeric.FileDataRetriever

/**
 * @author Carsten Hufe
 */
class HashCode64DataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "idx")
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
        def chunkSize = 32000
        def umcompressedFileLength = 20 * 11 * 1600 // index entry length * 11 chunks * datasets per chunk
        SnappyChunksUtil.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, chunkSize)
        SnappyChunksUtil.getSnappyChunksByFile(file)
    }

    def static createFileDataRetriever(file, snappyChunks) {
        new FileDataRetriever() {

            @Override
            BlockRange<Long> getBlockRange(long searchChunk) throws IOException {
                def ramFile = new RandomAccessFile(file, "r")
                byte[] uncompressedBlock = SnappyUtil.getUncompressed(ramFile, snappyChunks, searchChunk)
                Long firstInt = SnappyUtil.readLong(uncompressedBlock, 0);
                Long lastInt = SnappyUtil.readLong(uncompressedBlock, uncompressedBlock.length - 20);
                ramFile.close()
                return new BlockRange<Long>(firstInt, lastInt);

            }
        }
    }
}
