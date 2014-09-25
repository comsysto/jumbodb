package org.jumbodb.connector.hadoop.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.UnhandledException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.*;
import org.jumbodb.connector.hadoop.importer.input.JumboInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jumbodb.connector.hadoop.index.strategy.common.AbstractIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.integer.IntegerSamplingInputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:21 PM
 */
public class IndexJobCreator {

    public static IndexControlledJob createGenericIndexJob(Configuration conf, JumboGenericImportJob genericImportJob, IndexField indexField) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Path output = new Path(genericImportJob.getIndexOutputPath().toString() + "/" + indexField.getIndexName());
        Job job = Job.getInstance(conf, "Index " + indexField.getIndexName() + " Job " + genericImportJob.getInputPath());
        FileInputFormat.addInputPath(job, genericImportJob.getSortedInputPath());
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, false);
        IndexStrategy indexStrategy = getIndexStrategy(indexField);
        AbstractIndexMapper abstractIndexMapper = createInstance(indexStrategy.getMapperClass());
        String indexConfig = objectMapper.writeValueAsString(indexField);
        job.getConfiguration().set(JumboConfigurationUtil.JUMBO_INDEX_JSON_CONF, indexConfig);
        JumboInputFormat.setChecksumType(job, genericImportJob.getChecksumType());
        job.setJarByClass(IndexJobCreator.class);
        job.setMapperClass(indexStrategy.getMapperClass());
        job.setInputFormatClass(DataStrategies.getDataStrategy(genericImportJob.getDataStrategy()).getInputFormat());
        job.setNumReduceTasks(indexField.getNumberOfOutputFiles());
        job.setMapOutputValueClass(abstractIndexMapper.getOutputValueClass());
        job.setMapOutputKeyClass(abstractIndexMapper.getOutputKeyClass());
        job.setOutputFormatClass(indexStrategy.getOutputFormatClass());
        job.setOutputKeyClass(abstractIndexMapper.getOutputKeyClass());
        job.setOutputValueClass(abstractIndexMapper.getOutputValueClass());
        Class<? extends InputFormat> samplingInputClass = abstractIndexMapper.getPartitionerSamplingInputClass();
        if(samplingInputClass != null && indexField.isUseSampling()) {
            executeSampling(conf, genericImportJob, indexField, job, abstractIndexMapper, indexConfig, samplingInputClass);
        }
        else {
            job.setPartitionerClass(abstractIndexMapper.getPartitioner());
        }
        return new IndexControlledJob(new ControlledJob(job, new ArrayList<ControlledJob>()), output);
    }

    private static void executeSampling(Configuration conf, JumboGenericImportJob genericImportJob, IndexField indexField, Job job, AbstractIndexMapper abstractIndexMapper, String indexConfig, Class<? extends InputFormat> samplingInputClass) throws IOException {
        Job samplingJob = Job.getInstance(conf);
        samplingJob.setNumReduceTasks(indexField.getNumberOfOutputFiles());
        samplingJob.setMapOutputKeyClass(abstractIndexMapper.getOutputKeyClass());
        samplingJob.setInputFormatClass(samplingInputClass);
        samplingJob.setOutputKeyClass(NullWritable.class);
        samplingJob.getConfiguration().set(JumboConfigurationUtil.JUMBO_INDEX_JSON_CONF, indexConfig);

        IntegerSamplingInputFormat.addInputPath(samplingJob, genericImportJob.getInputPath());
        InputSampler.Sampler<Text, Text> sampler =
                new InputSampler.RandomSampler<Text, Text>(0.1, 1000000);
        Path partitionFile = new Path(genericImportJob.getIndexOutputPath().toString() + "/.partitions/" + indexField.getIndexName());
        TotalOrderPartitioner.setPartitionFile(samplingJob.getConfiguration(), partitionFile);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
        try {
            System.out.println("Starting sampling for index " + indexField);
            InputSampler.writePartitionFile(samplingJob, sampler);
            System.out.println("Finished sampling for index " + indexField);
        } catch (ClassNotFoundException e) {
            throw new UnhandledException(e);
        } catch (InterruptedException e) {
            throw new UnhandledException(e);
        } catch (ArrayIndexOutOfBoundsException e) {
            job.setPartitionerClass(abstractIndexMapper.getPartitioner());
            // workaround when no samples were found, writePartitionFile has a bug and throws array out of bound exception
            System.out.println("No sample values found for " + indexField + " falling back to default partitioner.");
        }
        job.setPartitionerClass(TotalOrderPartitioner.class);
    }

    private static IndexStrategy getIndexStrategy(IndexField indexField) {
        return IndexStrategies.getIndexStrategy(indexField.getIndexStrategy());
    }

    public static IndexControlledJob createCustomIndexJob(Configuration conf, JumboCustomImportJob customImportJob, Class<? extends AbstractIndexMapper> mapper, Class<? extends OutputFormat> outputFormat) throws IOException {
        AbstractIndexMapper abstractIndexMapper = createInstance(mapper);
        String indexName = abstractIndexMapper.getIndexName();
        Path output = new Path(customImportJob.getIndexOutputPath().toString() + "/" + indexName);
        Job job = Job.getInstance(conf, "Index " + mapper.getSimpleName() + " Job " + customImportJob.getSortedInputPath());
        FileInputFormat.addInputPath(job, customImportJob.getSortedInputPath());
        FileOutputFormat.setOutputPath(job, output);
        FileOutputFormat.setCompressOutput(job, false);
        job.setJarByClass(IndexJobCreator.class);
        job.setMapperClass(mapper);
        job.setInputFormatClass(DataStrategies.getDataStrategy(customImportJob.getDataStrategy()).getInputFormat());
        job.setMapOutputKeyClass(abstractIndexMapper.getOutputKeyClass());
        job.setMapOutputValueClass(abstractIndexMapper.getOutputValueClass());
        job.setOutputFormatClass(outputFormat);
        job.setOutputKeyClass(abstractIndexMapper.getOutputKeyClass());
        job.setOutputValueClass(abstractIndexMapper.getOutputValueClass());
        job.setPartitionerClass(abstractIndexMapper.getPartitioner());
        if(abstractIndexMapper.getPartitionerSamplingInputClass() != null) {
            throw new IllegalStateException("Sampling is not supported for custom jobs.");
        }
        JumboInputFormat.setChecksumType(job, customImportJob.getChecksumType());
        return new IndexControlledJob(new ControlledJob(job, new ArrayList<ControlledJob>()), output);
    }

    public static IndexField getIndexInformation(Class<? extends AbstractIndexMapper> mapper, String strategy, int numberOfFiles) {
        AbstractIndexMapper instance = createInstance(mapper);
        return new IndexField(instance.getIndexName(), instance.getIndexSourceFields(), strategy, numberOfFiles);
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
