package org.jumbodb.connector.hadoop.index;

import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.json.IndexJson;
import org.jumbodb.connector.hadoop.index.map.AbstractHashCodeIndexMapper;
import org.jumbodb.connector.hadoop.index.map.GenericJsonHashCodeIndexMapper;
import org.jumbodb.connector.hadoop.index.output.BinaryIndexOutputFormat;
import org.jumbodb.connector.hadoop.index.output.HashRangePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

    public static final String HASHCODE_SNAPPY_V_1 = "hashcode_snappy_v1";

    public static Map<String, Class<? extends Mapper>> GENERIC_INDEX_MAPPER_STRATEGIES = createIndexMapper();

    private static Map<String, Class<? extends Mapper>> createIndexMapper() {
        Map<String, Class<? extends Mapper>> indexMapper = new HashMap<String, Class<? extends Mapper>>();
        indexMapper.put(HASHCODE_SNAPPY_V_1, GenericJsonHashCodeIndexMapper.class);
        return Collections.unmodifiableMap(indexMapper);
    }

    public static IndexControlledJob createGenericIndexJob(Configuration conf, IndexJson indexJson, Path jsonDataToIndex, Path outputPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Path output = new Path(outputPath.toString() + "/" + indexJson.getIndexName());
        Job job = new Job(conf, "Index " + indexJson.getIndexName() + " Job " + jsonDataToIndex);
        FileInputFormat.addInputPath(job, jsonDataToIndex);
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, false);
        job.getConfiguration().set(GenericJsonHashCodeIndexMapper.JUMBO_INDEX_JSON_CONF, objectMapper.writeValueAsString(indexJson));
        job.setJarByClass(IndexJobCreator.class);
        Class<? extends Mapper> indexMapper = GENERIC_INDEX_MAPPER_STRATEGIES.get(indexJson.getStrategy());
        if(indexMapper == null) {
            throw new IllegalStateException("Index mapper strategy is not available " + indexJson.getStrategy());
        }
        job.setMapperClass(indexMapper);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FileOffsetWritable.class);
        job.setOutputFormatClass(BinaryIndexOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FileOffsetWritable.class);
        job.setPartitionerClass(HashRangePartitioner.class);
        return new IndexControlledJob(new ControlledJob(job, new ArrayList<ControlledJob>()), output);
    }

    public static IndexControlledJob createIndexJob(Configuration conf, Class<? extends AbstractHashCodeIndexMapper> mapper, Path jsonDataToIndex, Path outputPath) throws IOException {
        AbstractHashCodeIndexMapper abstractHashCodeIndexMapper = createInstance(mapper);
        String indexName = abstractHashCodeIndexMapper.getIndexName();
        Path output = new Path(outputPath.toString() + "/" + indexName);
        Job job = new Job(conf, "Index " + mapper.getSimpleName() + " Job " + jsonDataToIndex);
        FileInputFormat.addInputPath(job, jsonDataToIndex);
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

    private static AbstractHashCodeIndexMapper createInstance(Class<? extends AbstractHashCodeIndexMapper> mapper)  {
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
