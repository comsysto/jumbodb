package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexMapper;

/**
 * Created by Carsten on 21.09.2014.
 */
public class IndexStrategy {
    private Class<? extends AbstractIndexMapper> mapperClass;
    private Class<? extends OutputFormat> outputFormatClass;
    private int numberOfOutputFiles;

    public IndexStrategy(Class<? extends AbstractIndexMapper> mapperClass, Class<? extends OutputFormat> outputFormatClass, int numberOfOutputFiles) {
        this.mapperClass = mapperClass;
        this.outputFormatClass = outputFormatClass;
        this.numberOfOutputFiles = numberOfOutputFiles;
    }

    public Class<? extends AbstractIndexMapper> getMapperClass() {
        return mapperClass;
    }

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    public int getNumberOfOutputFiles() {
        return numberOfOutputFiles;
    }

    @Override
    public String toString() {
        return "IndexStrategy{" +
                "mapperClass=" + mapperClass +
                ", outputFormatClass=" + outputFormatClass +
                ", numberOfOutputFiles=" + numberOfOutputFiles +
                '}';
    }
}
