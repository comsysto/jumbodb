package org.jumbodb.connector.hadoop.index.strategy.geohash.snappy;

import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class GeohashIndexOutputFormat extends AbstractIndexOutputFormat<IntWritable, GeoFileOffsetWritable> {

    @Override
    protected void write(IntWritable k, GeoFileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeInt(k.get());
        out.writeFloat((float) v.getLatitude());
        out.writeFloat((float) v.getLongitude());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }
}
