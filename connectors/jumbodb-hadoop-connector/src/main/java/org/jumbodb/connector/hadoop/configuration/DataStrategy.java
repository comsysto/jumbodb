package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;

/**
 * @author Carsten Hufe
 */
public abstract class DataStrategy {
    private Class<? extends InputFormat> inputFormat;
    private Class<? extends OutputFormat> outputFormat;
    private Class<?> outputClass;

    public DataStrategy(Class<? extends InputFormat> inputFormat, Class<? extends OutputFormat> outputFormat, Class<?> outputClass) {
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.outputClass = outputClass;
    }

    public Class<? extends InputFormat> getInputFormat() {
        return inputFormat;
    }

    public Class<? extends OutputFormat> getOutputFormat() {
        return outputFormat;
    }

    public Class<?> getOutputClass() {
        return outputClass;
    }

    public abstract Class<?> getSortOutputKeyClassByType(String type);
    public abstract Class<? extends Mapper> getSortMapperByType(String type);
}