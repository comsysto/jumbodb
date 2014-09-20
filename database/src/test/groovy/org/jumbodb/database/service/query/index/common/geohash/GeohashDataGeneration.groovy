package org.jumbodb.database.service.query.index.common.geohash

import org.jumbodb.common.geo.geohash.GeoHash
import org.jumbodb.data.common.compression.CompressionBlocksUtil
import org.jumbodb.data.common.compression.CompressionUtil
import org.jumbodb.data.common.snappy.SnappyUtil
import org.jumbodb.database.service.query.index.common.BlockRange
import org.jumbodb.database.service.query.index.common.numeric.FileDataRetriever

/**
 * @author Carsten Hufe
 */
class GeohashDataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "idx")
    }

    def static createIndexContent() {
        def fos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(fos)

        // write 10 blocks

        def fileHash = 50000
        def offsetBase = 100000
        def i = 0
        def result = []
        for(chunks in 1..10) {
            for(datasetInChunk in 1..2048) {
                def lat = chunks.floatValue()
                def lon = datasetInChunk.floatValue() / 100f
                def geohash =  GeoHash.withBitPrecision(lat, lon, 32).intValue();
                def val = [geohash: geohash, lat: lat, lon: lon, i: i]
                result.add(val)
//                println(val)
                i++
            }
//            println("============================")
        }
        result = result.sort{[it.geohash]}
        for(dataset in result) {
            dos.writeInt(dataset.geohash)
            dos.writeFloat(dataset.lat.floatValue())
            dos.writeFloat(dataset.lon.floatValue())
            dos.writeInt(fileHash)
            dos.writeLong(dataset.i + offsetBase)
        }
        dos.close()
        fos.close()
        fos.toByteArray()
    }

    def static createIndexFile(file) {
        def chunkSize = 24 * 2048
        def umcompressedFileLength = 24 * 10 * 2048 // index entry length * 10 blocks * datasets per chunk
        SnappyUtil.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, 100l, chunkSize)
        CompressionBlocksUtil.getBlocksByFile(file)
    }

    def static createFileDataRetriever(file, snappyChunks) {
        new FileDataRetriever() {

            @Override
            BlockRange<GeohashCoords> getBlockRange(long searchChunk) throws IOException {
                def ramFile = new RandomAccessFile(file, "r")
                byte[] uncompressedBlock = SnappyUtil.getUncompressed(ramFile, snappyChunks, searchChunk)
                GeohashCoords firstInt = readFirstValue(uncompressedBlock);
                GeohashCoords lastInt = readLastValue(uncompressedBlock);
                ramFile.close()
                return new BlockRange<GeohashCoords>(firstInt, lastInt);

            }

            def readLastValue(byte[] uncompressed) {
                int geohash = CompressionUtil.readInt(uncompressed, uncompressed.length - 24);
                float latitude = CompressionUtil.readFloat(uncompressed, uncompressed.length - 20);
                float longitude = CompressionUtil.readFloat(uncompressed, uncompressed.length - 16);
                return new GeohashCoords(geohash, latitude, longitude);
            }

            def readFirstValue(byte[] uncompressed) {
                int geohash = CompressionUtil.readInt(uncompressed, 0);
                float latitude = CompressionUtil.readFloat(uncompressed, 4);
                float longitude = CompressionUtil.readFloat(uncompressed, 8);
                return new GeohashCoords(geohash, latitude, longitude);
            }
        }
    }
}
