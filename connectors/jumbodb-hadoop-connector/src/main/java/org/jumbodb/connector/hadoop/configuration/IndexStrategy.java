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
    private Class<? extends InputFormat> inputSamplingMapper;
    private int numberOfOutputFiles;

    public IndexStrategy(Class<? extends AbstractIndexMapper> mapperClass, Class<? extends OutputFormat> outputFormatClass, Class<? extends InputFormat> inputSamplingMapper, int numberOfOutputFiles) {
        this.mapperClass = mapperClass;
        this.outputFormatClass = outputFormatClass;
        this.inputSamplingMapper = inputSamplingMapper;
        this.numberOfOutputFiles = numberOfOutputFiles;
    }

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

    public Class<? extends InputFormat> getInputSamplingMapper() {
        return inputSamplingMapper;
    }

    @Override
    public String toString() {
        return "IndexStrategy{" +
                "mapperClass=" + mapperClass +
                ", outputFormatClass=" + outputFormatClass +
                ", inputSamplingMapper=" + inputSamplingMapper +
                ", numberOfOutputFiles=" + numberOfOutputFiles +
                '}';
    }
}
