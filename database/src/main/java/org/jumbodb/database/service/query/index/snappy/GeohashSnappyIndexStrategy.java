package org.jumbodb.database.service.query.index.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.geohash.GeohashBoundaryBoxOperationSearch;
import org.jumbodb.database.service.query.index.common.geohash.GeohashCoords;
import org.jumbodb.database.service.query.index.common.geohash.GeohashWithinRangeMeterBoxOperationSearch;
import org.jumbodb.data.common.snappy.SnappyUtil;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class GeohashSnappyIndexStrategy extends NumberSnappyIndexStrategy<GeohashCoords, Integer, NumberIndexFile<Integer>> {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 48 * 1024; // must be a multiple of 24! (4 byte geo hash, 4 byte latitude, 4 byte longitude, 4 byte file name hash, 8 byte offset)

    @Override
    public Map<QueryOperation, IndexOperationSearch<GeohashCoords, Integer, NumberIndexFile<Integer>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<GeohashCoords, Integer, NumberIndexFile<Integer>>> operations = new HashMap<QueryOperation, IndexOperationSearch<GeohashCoords, Integer, NumberIndexFile<Integer>>>();
        operations.put(QueryOperation.GEO_BOUNDARY_BOX, new GeohashBoundaryBoxOperationSearch());
        operations.put(QueryOperation.GEO_WITHIN_RANGE_METER, new GeohashWithinRangeMeterBoxOperationSearch());
        return operations;
    }

    @Override
    public int getSnappyChunkSize() {
        return SNAPPY_INDEX_CHUNK_SIZE;
    }

    @Override
    public GeohashCoords readValueFromDataInput(DataInput dis) throws IOException {
        return new GeohashCoords(dis.readInt(), dis.readFloat(), dis.readFloat());
    }

    @Override
    public GeohashCoords readLastValue(byte[] uncompressed) {
        int geohash = SnappyUtil.readInt(uncompressed, uncompressed.length - 24);
        float latitude = SnappyUtil.readFloat(uncompressed, uncompressed.length - 20);
        float longitude = SnappyUtil.readFloat(uncompressed, uncompressed.length - 16);
        return new GeohashCoords(geohash, latitude, longitude);
    }

    @Override
    public GeohashCoords readFirstValue(byte[] uncompressed) {
        int geohash = SnappyUtil.readInt(uncompressed, 0);
        float latitude = SnappyUtil.readFloat(uncompressed, 4);
        float longitude = SnappyUtil.readFloat(uncompressed, 8);
        return new GeohashCoords(geohash, latitude, longitude);
//        return SnappyUtil.readInt(uncompressed, 0);
    }

    @Override
    public NumberIndexFile<Integer> createIndexFile(GeohashCoords from, GeohashCoords to, File indexFile) {
        return new NumberIndexFile<Integer>(from.getGeohash(), to.getGeohash(), indexFile);
    }

    @Override
    public String getStrategyName() {
        return "GEOHASH_SNAPPY";
    }
}