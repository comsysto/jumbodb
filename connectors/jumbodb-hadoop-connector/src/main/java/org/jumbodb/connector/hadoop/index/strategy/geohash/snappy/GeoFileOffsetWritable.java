package org.jumbodb.connector.hadoop.index.strategy.geohash.snappy;

import org.apache.hadoop.io.DoubleWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public class GeoFileOffsetWritable extends FileOffsetWritable {
    double latitude;
    double longitude;

    public GeoFileOffsetWritable() {
    }

    public GeoFileOffsetWritable(int fileNameHashCode, long offset, double latitude, double longitude) {
        super(fileNameHashCode, offset);
        this.latitude = latitude;
        this.longitude = longitude;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeDouble(latitude);
        dataOutput.writeDouble(longitude);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        latitude = dataInput.readDouble();
        longitude = dataInput.readDouble();
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
}


