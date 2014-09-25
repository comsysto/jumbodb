package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.jumbodb.connector.hadoop.index.strategy.common.AbstractIndexMapper;

/**
 * Created by Carsten on 21.09.2014.
 */
public class IndexStrategy {
    private Class<? extends AbstractIndexMapper> mapperClass;
    private Class<? extends OutputFormat> outputFormatClass;

    public IndexStrategy(Class<? extends AbstractIndexMapper> mapperClass, Class<? extends OutputFormat> outputFormatClass) {
        this.mapperClass = mapperClass;
        this.outputFormatClass = outputFormatClass;
    }

    public Class<? extends AbstractIndexMapper> getMapperClass() {
        return mapperClass;
    }

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    @Override
    public String toString() {
        return "IndexStrategy{" +
                "mapperClass=" + mapperClass +
                ", outputFormatClass=" + outputFormatClass +
                '}';
    }
}
