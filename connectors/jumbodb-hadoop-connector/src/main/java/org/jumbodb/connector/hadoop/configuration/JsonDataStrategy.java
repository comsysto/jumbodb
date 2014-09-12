package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;

/**
 * Carsten Hufe
 */
public class JsonDataStrategy extends DataStrategy {
    public JsonDataStrategy(final Class<? extends InputFormat> inputFormat,
      final Class<? extends OutputFormat> outputFormat, final Class<?> outputClass) {
        super(inputFormat, outputFormat, outputClass);
    }

    @Override
    public Class<?> getSortOutputKeyClassByType(final String type) {
        return JumboConfigurationUtil.getSortOutputKeyClassByType(type);
    }

    @Override
    public Class<? extends Mapper> getSortMapperByType(final String type) {
        return JumboConfigurationUtil.getSortMapperByType(type);
    }
}
