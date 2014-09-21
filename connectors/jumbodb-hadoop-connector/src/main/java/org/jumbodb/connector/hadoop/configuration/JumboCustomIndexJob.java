package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.mapreduce.OutputFormat;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexMapper;

/**
 * Created by Carsten on 21.09.2014.
 */
public class JumboCustomIndexJob {
    private Class<? extends AbstractIndexMapper> mapper;
    private Class<? extends OutputFormat> outputFormat;
    private String strategy;
    private int numberOfFiles;

    public JumboCustomIndexJob() {
    }

    public JumboCustomIndexJob(Class<? extends AbstractIndexMapper> mapper, Class<? extends OutputFormat> outputFormat, String strategy, int numberOfFiles) {
        this.mapper = mapper;
        this.outputFormat = outputFormat;
        this.strategy = strategy;
        this.numberOfFiles = numberOfFiles;
    }

    public Class<? extends AbstractIndexMapper> getMapper() {
        return mapper;
    }

    public void setMapper(Class<? extends AbstractIndexMapper> mapper) {
        this.mapper = mapper;
    }

    public Class<? extends OutputFormat> getOutputFormat() {
        return outputFormat;
    }

    public void setOutputFormat(Class<? extends OutputFormat> outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public int getNumberOfFiles() {
        return numberOfFiles;
    }

    public void setNumberOfFiles(int numberOfFiles) {
        this.numberOfFiles = numberOfFiles;
    }
}
