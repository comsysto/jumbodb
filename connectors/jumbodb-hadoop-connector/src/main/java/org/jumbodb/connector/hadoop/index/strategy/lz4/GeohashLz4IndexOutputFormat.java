package org.jumbodb.connector.hadoop.index.strategy.lz4;

import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.index.strategy.common.geohash.GeoFileOffsetWritable;

import java.io.DataOutputStream;
import java.io.IOException;

public class GeohashLz4IndexOutputFormat extends AbstractLz4IndexOutputFormat<IntWritable, GeoFileOffsetWritable> {
    public static final String GEOHASH_LZ4 = "GEOHASH_LZ4";

    @Override
    protected void write(IntWritable k, GeoFileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeInt(k.get());
        out.writeFloat((float) v.getLatitude());
        out.writeFloat((float) v.getLongitude());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getLz4BlockSize() {
        return 48 * 1024; // must be a multiple of 24! (4 byte geo hash, 4 byte latitude, 4 byte longitude, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return GEOHASH_LZ4;
    }
}
