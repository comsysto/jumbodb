package org.jumbodb.connector.hadoop.index.strategy.common.geohash;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.snappy.GeohashSnappyIndexOutputFormat;

import java.io.IOException;
import java.util.List;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractGeohashIndexMapper<T> extends AbstractIndexMapper<T> {
    private IntWritable keyW = new IntWritable();
    private GeoFileOffsetWritable valueW = new GeoFileOffsetWritable();

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        List<Double> indexableValue = getIndexableValue(input);
        if(indexableValue != null && indexableValue.size() == 2) {
            Double latitude = indexableValue.get(0);
            Double longitude = indexableValue.get(1);
            GeoHash geoHash = GeoHash.withBitPrecision(latitude, longitude, 32);
            keyW.set(geoHash.intValue());
            valueW.setLatitude(latitude);
            valueW.setLongitude(longitude);
            valueW.setFileNameHashCode(fileNameHashCode);
            valueW.setOffset(offset.get());
            context.write(keyW, valueW);
        }
    }

    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return GeohashRangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return IntWritable.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
        return GeoFileOffsetWritable.class;
    }

    public abstract List<Double> getIndexableValue(T input);
}
