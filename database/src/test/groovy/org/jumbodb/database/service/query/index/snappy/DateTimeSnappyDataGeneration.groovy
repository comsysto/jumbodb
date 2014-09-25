package org.jumbodb.database.service.query.index.snappy

import org.apache.commons.lang.time.DateUtils
import org.jumbodb.data.common.compression.CompressionBlocksUtil
import org.jumbodb.data.common.compression.CompressionUtil
import org.jumbodb.data.common.snappy.SnappyUtil
import org.jumbodb.database.service.query.index.common.BlockRange
import org.jumbodb.database.service.query.index.common.numeric.FileDataRetriever

/**
 * @author Carsten Hufe
 */
class DateTimeSnappyDataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "idx")
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
        SnappyUtil.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, 100l, chunkSize)
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