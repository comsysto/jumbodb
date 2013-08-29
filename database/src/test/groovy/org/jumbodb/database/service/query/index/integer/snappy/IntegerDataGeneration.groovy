package org.jumbodb.database.service.query.index.integer.snappy

import org.jumbodb.data.common.snappy.SnappyChunksUtil
import org.jumbodb.data.common.snappy.SnappyUtil
import org.jumbodb.database.service.query.index.basic.numeric.BlockRange
import org.jumbodb.database.service.query.index.basic.numeric.FileDataRetriever

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

    def static createFileDataRetriever(file, snappyChunks) {
        new FileDataRetriever() {

            @Override
            BlockRange<Integer> getBlockRange(long searchChunk) throws IOException {
                def ramFile = new RandomAccessFile(file, "r")
                byte[] uncompressedBlock = SnappyUtil.getUncompressed(ramFile, snappyChunks, searchChunk)
                Integer firstInt = SnappyUtil.readInt(uncompressedBlock, 0);
                Integer lastInt = SnappyUtil.readInt(uncompressedBlock, uncompressedBlock.length - 16);
                ramFile.close()
                return new BlockRange<Integer>(firstInt, lastInt);

            }
        }
    }
}
