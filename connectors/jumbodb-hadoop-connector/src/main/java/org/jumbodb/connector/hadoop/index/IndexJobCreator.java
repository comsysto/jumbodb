package org.jumbodb.connector.hadoop.index;

import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;
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

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:21 PM
 */
public class IndexJobCreator {

    public static IndexControlledJob createIndexJob(Configuration conf, Class<? extends AbstractIndexMapper> mapper, Path jsonDataToIndex, Path outputPath) throws IOException {
        AbstractIndexMapper abstractIndexMapper = createInstance(mapper);
        String indexName = abstractIndexMapper.getIndexName();
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
