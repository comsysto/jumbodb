package org.jumbodb.connector.hadoop.index;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.configuration.JumboCustomImportJob;
import org.jumbodb.connector.hadoop.configuration.JumboGenericImportJob;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;
import org.jumbodb.connector.hadoop.index.map.GenericJsonHashCodeIndexMapper;
import org.jumbodb.connector.hadoop.index.map.GenericJsonIntegerIndexMapper;
import org.jumbodb.connector.hadoop.index.output.BinaryIndexOutputFormat;
import org.jumbodb.connector.hadoop.index.output.HashRangePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jumbodb.connector.hadoop.index.output.IntegerRangePartitioner;

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

    public static Map<String, Class<? extends Mapper>> INDEX_MAPPER_STRATEGIES = createIndexMapper();
    public static Map<String, Class<? extends Partitioner>> INDEX_PARTITIONER_STRATEGIES = createIndexPartitioner();
    public static Map<String, Class<? extends Writable>> INDEX_OUTPUT_KEY_STRATEGIES = createOutputKeyClasses();

    private static Map<String, Class<? extends Mapper>> createIndexMapper() {
        Map<String, Class<? extends Mapper>> indexMapper = new HashMap<String, Class<? extends Mapper>>();
        indexMapper.put(GenericJsonHashCodeIndexMapper.HASHCODE_SNAPPY_V_1, GenericJsonHashCodeIndexMapper.class);
        indexMapper.put(GenericJsonIntegerIndexMapper.INTEGER_SNAPPY_V_1, GenericJsonIntegerIndexMapper.class);
        return Collections.unmodifiableMap(indexMapper);
    }

    private static Map<String, Class<? extends Partitioner>> createIndexPartitioner() {
        Map<String, Class<? extends Partitioner>> indexPartitioner = new HashMap<String, Class<? extends Partitioner>>();
        indexPartitioner.put(GenericJsonHashCodeIndexMapper.HASHCODE_SNAPPY_V_1, HashRangePartitioner.class);
        indexPartitioner.put(GenericJsonIntegerIndexMapper.INTEGER_SNAPPY_V_1, IntegerRangePartitioner.class);
        return Collections.unmodifiableMap(indexPartitioner);
    }

    private static Map<String, Class<? extends Writable>> createOutputKeyClasses() {
        Map<String, Class<? extends Writable>> outputKeyClasses = new HashMap<String, Class<? extends Writable>>();
        outputKeyClasses.put(GenericJsonHashCodeIndexMapper.HASHCODE_SNAPPY_V_1, IntWritable.class);
        outputKeyClasses.put(GenericJsonIntegerIndexMapper.INTEGER_SNAPPY_V_1, IntWritable.class);
        return Collections.unmodifiableMap(outputKeyClasses);
    }

    public static IndexControlledJob createGenericIndexJob(Configuration conf, JumboGenericImportJob genericImportJob, IndexField indexField) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Path output = new Path(genericImportJob.getIndexOutputPath().toString() + "/" + indexField.getIndexName());
        Job job = new Job(conf, "Index " + indexField.getIndexName() + " Job " + genericImportJob.getInputPath());
        FileInputFormat.addInputPath(job, genericImportJob.getSortedInputPath());
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, false);
        job.getConfiguration().set(GenericJsonHashCodeIndexMapper.JUMBO_INDEX_JSON_CONF, objectMapper.writeValueAsString(indexField));
        job.setJarByClass(IndexJobCreator.class);
        job.setMapperClass(getMapperStrategy(indexField));
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FileOffsetWritable.class);
        job.setOutputFormatClass(BinaryIndexOutputFormat.class);
        job.setOutputKeyClass(getOutputKeyClassStrategy(indexField));
        job.setOutputValueClass(FileOffsetWritable.class);
        job.setPartitionerClass(getPartitionerStrategy(indexField));
        return new IndexControlledJob(new ControlledJob(job, new ArrayList<ControlledJob>()), output);
    }

    private static Class<? extends Partitioner> getPartitionerStrategy(IndexField indexField) {
        Class<? extends Partitioner> indexPartitioner = INDEX_PARTITIONER_STRATEGIES.get(indexField.getIndexStrategy());
        if(indexPartitioner == null) {
            throw new IllegalStateException("Index partitioner strategy is not available " + indexField.getIndexStrategy());
        }
        return indexPartitioner;
    }

    private static Class<? extends Writable> getOutputKeyClassStrategy(IndexField indexField) {
        Class<? extends Writable> outputKeyClass = INDEX_OUTPUT_KEY_STRATEGIES.get(indexField.getIndexStrategy());
        if(outputKeyClass == null) {
            throw new IllegalStateException("Index Output Key Class strategy is not available " + indexField.getIndexStrategy());
        }
        return outputKeyClass;
    }

    private static Class<? extends Mapper> getMapperStrategy(IndexField indexField) {
        Class<? extends Mapper> indexMapper = INDEX_MAPPER_STRATEGIES.get(indexField.getIndexStrategy());
        if(indexMapper == null) {
            throw new IllegalStateException("Index mapper strategy is not available " + indexField.getIndexStrategy());
        }
        return indexMapper;
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
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FileOffsetWritable.class);
        job.setOutputFormatClass(BinaryIndexOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FileOffsetWritable.class);
        job.setPartitionerClass(HashRangePartitioner.class);
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
