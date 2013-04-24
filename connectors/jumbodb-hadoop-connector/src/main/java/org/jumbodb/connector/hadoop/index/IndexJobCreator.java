package org.jumbodb.connector.hadoop.index;

import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.configuration.JumboCustomImportJob;
import org.jumbodb.connector.hadoop.configuration.JumboGenericImportJob;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.doubleval.snappy.GenericJsonDoubleIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.floatval.snappy.GenericJsonFloatIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.hashcode.snappy.GenericJsonHashCodeIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.hashcode.snappy.HashCodeIndexOutputFormat;
import org.jumbodb.connector.hadoop.index.strategy.hashcode.snappy.HashRangePartitioner;
import org.jumbodb.connector.hadoop.index.strategy.integer.snappy.GenericJsonIntegerIndexMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jumbodb.connector.hadoop.index.strategy.integer.snappy.IntegerRangePartitioner;
import org.jumbodb.connector.hadoop.index.strategy.longval.snappy.GenericJsonLongIndexMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:21 PM
 */
public class IndexJobCreator {

    public static Map<String, Class<? extends AbstractIndexMapper>> INDEX_STRATEGIES = createIndexMapper();

    private static Map<String, Class<? extends AbstractIndexMapper>> createIndexMapper() {
        Map<String, Class<? extends AbstractIndexMapper>> indexMapper = new HashMap<String, Class<? extends AbstractIndexMapper>>();
        indexMapper.put(GenericJsonHashCodeIndexMapper.HASHCODE_SNAPPY_V_1, GenericJsonHashCodeIndexMapper.class);
        indexMapper.put(GenericJsonIntegerIndexMapper.INTEGER_SNAPPY_V_1, GenericJsonIntegerIndexMapper.class);
        indexMapper.put(GenericJsonLongIndexMapper.LONG_SNAPPY_V1, GenericJsonLongIndexMapper.class);
        indexMapper.put(GenericJsonFloatIndexMapper.FLOAT_SNAPPY_V_1, GenericJsonFloatIndexMapper.class);
        indexMapper.put(GenericJsonDoubleIndexMapper.DOUBLE_SNAPPY_V_1, GenericJsonDoubleIndexMapper.class);
        return Collections.unmodifiableMap(indexMapper);
    }

    public static IndexControlledJob createGenericIndexJob(Configuration conf, JumboGenericImportJob genericImportJob, IndexField indexField) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Path output = new Path(genericImportJob.getIndexOutputPath().toString() + "/" + indexField.getIndexName());
        Job job = new Job(conf, "Index " + indexField.getIndexName() + " Job " + genericImportJob.getInputPath());
        FileInputFormat.addInputPath(job, genericImportJob.getSortedInputPath());
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, false);
        Class<? extends AbstractIndexMapper> mapper = getIndexStrategy(indexField);
        AbstractIndexMapper abstractIndexMapper = createInstance(mapper);
        job.getConfiguration().set(GenericJsonHashCodeIndexMapper.JUMBO_INDEX_JSON_CONF, objectMapper.writeValueAsString(indexField));
        job.setJarByClass(IndexJobCreator.class);
        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FileOffsetWritable.class);
        job.setOutputFormatClass(abstractIndexMapper.getOutputFormat());
        job.setOutputKeyClass(abstractIndexMapper.getOutputKeyClass());
        job.setOutputValueClass(FileOffsetWritable.class);
        job.setPartitionerClass(abstractIndexMapper.getPartitioner());
        return new IndexControlledJob(new ControlledJob(job, new ArrayList<ControlledJob>()), output);
    }

    private static Class<? extends AbstractIndexMapper> getIndexStrategy(IndexField indexField) {
        Class<? extends AbstractIndexMapper> strategy = INDEX_STRATEGIES.get(indexField.getIndexStrategy());
        if(strategy == null) {
            throw new IllegalStateException("Index strategy is not available " + indexField.getIndexStrategy());
        }
        return strategy;
    }

    public static IndexControlledJob createCustomIndexJob(Configuration conf, JumboCustomImportJob customImportJob, Class<? extends AbstractIndexMapper> mapper) throws IOException {
        AbstractIndexMapper abstractIndexMapper = createInstance(mapper);
        String indexName = abstractIndexMapper.getIndexName();
        Path output = new Path(customImportJob.getIndexOutputPath().toString() + "/" + indexName);
        Job job = new Job(conf, "Index " + mapper.getSimpleName() + " Job " + customImportJob.getSortedInputPath());
        FileInputFormat.addInputPath(job, customImportJob.getSortedInputPath());
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, false);
        job.setJarByClass(IndexJobCreator.class);
        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(abstractIndexMapper.getOutputKeyClass());
        job.setMapOutputValueClass(FileOffsetWritable.class);
        job.setOutputFormatClass(abstractIndexMapper.getOutputFormat());
        job.setOutputKeyClass(abstractIndexMapper.getOutputKeyClass());
        job.setOutputValueClass(FileOffsetWritable.class);
        job.setPartitionerClass(abstractIndexMapper.getPartitioner());
        return new IndexControlledJob(new ControlledJob(job, new ArrayList<ControlledJob>()), output);
    }

    public static IndexField getIndexInformation(Class<? extends AbstractIndexMapper> mapper) {
        AbstractIndexMapper instance = createInstance(mapper);
        return new IndexField(instance.getIndexName(), instance.getIndexSourceFields(), instance.getStrategy());
    }

    private static AbstractIndexMapper createInstance(Class<? extends AbstractIndexMapper> mapper)  {
        try {
            return mapper.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static class IndexControlledJob {
        private ControlledJob controlledJob;
        private Path indexPath;

        public IndexControlledJob(ControlledJob controlledJob, Path indexPath) {
            this.controlledJob = controlledJob;
            this.indexPath = indexPath;
        }

        public ControlledJob getControlledJob() {
            return controlledJob;
        }

        public Path getIndexPath() {
            return indexPath;
        }

        @Override
        public String toString() {
            return "IndexControlledJob{" +
                    "controlledJob=" + controlledJob +
                    ", indexPath=" + indexPath +
                    '}';
        }
    }
}
