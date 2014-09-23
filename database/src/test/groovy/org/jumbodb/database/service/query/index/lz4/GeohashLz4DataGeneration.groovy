package org.jumbodb.database.service.query.index.lz4

import org.jumbodb.common.geo.geohash.GeoHash
import org.jumbodb.data.common.compression.CompressionBlocksUtil
import org.jumbodb.data.common.lz4.Lz4Util

/**
 * @author Carsten Hufe
 */
class GeohashLz4DataGeneration {
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
        for (blocks in 1..10) {
            for (datasetInBlock in 1..2048) {
                def lat = blocks.floatValue()
                def lon = datasetInBlock.floatValue() / 100f
                def geohash = GeoHash.withBitPrecision(lat, lon, 32).intValue();
                def val = [geohash: geohash, lat: lat, lon: lon, i: i]
                result.add(val)
                i++
            }
        }
        result = result.sort { [it.geohash] }
        for (dataset in result) {
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
        def blockSize = 24 * 2048
        def uncompressedFileLength = 24 * 10 * 2048 // index entry length * 10 blocks * datasets per chunk
        Lz4Util.copy(new ByteArrayInputStream(createIndexContent()), file, uncompressedFileLength, 100l, blockSize)
        CompressionBlocksUtil.getBlocksByFile(file)
    }
}
