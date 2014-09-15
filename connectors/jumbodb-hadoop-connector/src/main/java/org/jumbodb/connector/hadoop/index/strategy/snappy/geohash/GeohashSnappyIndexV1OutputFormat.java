package org.jumbodb.connector.hadoop.index.strategy.snappy.geohash;

import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.index.output.index.AbstractSnappyIndexV1OutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

// CARSTEN remove version from name
public class GeohashSnappyIndexV1OutputFormat extends AbstractSnappyIndexV1OutputFormat<IntWritable, GeoFileOffsetWritable> {

    @Override
    protected void write(IntWritable k, GeoFileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeInt(k.get());
        out.writeFloat((float) v.getLatitude());
        out.writeFloat((float) v.getLongitude());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getSnappyBlockSize() {
        return 48 * 1024; // must be a multiple of 24! (4 byte geo hash, 4 byte latitude, 4 byte longitude, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return AbstractGeohashIndexMapper.GEOHASH_SNAPPY_V1;
    }
}
