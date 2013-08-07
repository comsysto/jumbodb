package org.jumbodb.database.service.query.index.geohash.snappy

import org.jumbodb.common.geo.geohash.GeoHash
import org.jumbodb.data.common.snappy.SnappyChunksUtil
import org.jumbodb.data.common.snappy.SnappyStreamToFileCopy

/**
 * @author Carsten Hufe
 */
class GeohashDataGeneration {
    def static createFile() {
        File.createTempFile("randomindex", "odx")
    }

    def static createIndexContent() {
        def fos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(fos)

        // write 10 chunks

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
        def umcompressedFileLength = 24 * 10 * 2048 // index entry length * 10 chunks * datasets per chunk
        SnappyStreamToFileCopy.copy(new ByteArrayInputStream(createIndexContent()), file, umcompressedFileLength, chunkSize)
        SnappyChunksUtil.getSnappyChunksByFile(file)
    }
}
