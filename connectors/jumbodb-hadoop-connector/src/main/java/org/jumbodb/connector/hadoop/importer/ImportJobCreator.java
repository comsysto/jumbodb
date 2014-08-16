package org.jumbodb.connector.hadoop.importer;

import org.jumbodb.connector.hadoop.JumboConstants;
import org.jumbodb.connector.hadoop.JumboJobCreator;
import org.jumbodb.connector.hadoop.importer.input.JumboInputFormat;
import org.jumbodb.connector.hadoop.importer.map.JumboImportMapper;
import org.jumbodb.connector.hadoop.importer.output.JumboOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.jumbodb.connector.hadoop.configuration.BaseJumboImportJob;
import org.jumbodb.connector.hadoop.configuration.ImportHost;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:21 PM
 */
public class ImportJobCreator {

    private static ControlledJob createJumboJob(Configuration conf, Path importPath, Path reportOutputPath, String type, BaseJumboImportJob genericImportJob, ImportHost importHost, IndexField indexField) throws IOException {
        String jobName = "jumboDB Import " + importHost.getHost() + " " + importPath.toString() + ":" + type;
        System.out.println(jobName);
        Job job = Job.getInstance(conf, jobName);
        JumboInputFormat.setDataType(job, type);
        JumboInputFormat.setImportPath(job, importPath);
        JumboInputFormat.setIndexName(job, indexField != null ? indexField.getIndexName() : "not_set");
        JumboInputFormat.setCollectionName(job, genericImportJob.getCollectionName());
        JumboInputFormat.setDeliveryChunkKey(job, genericImportJob.getDeliveryChunkKey());
        FileOutputFormat.setOutputPath(job, reportOutputPath);
        FileInputFormat.addInputPath(job, importPath);
        job.setJarByClass(ImportJobCreator.class);
        job.setInputFormatClass(JumboInputFormat.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class); // file name
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class); // file name
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(JumboImportMapper.class);
        job.setOutputFormatClass(JumboOutputFormat.class);
        job.setNumReduceTasks(1);
        job.setSpeculativeExecution(false);
        job.setMapSpeculativeExecution(false);
        Configuration jobConf = job.getConfiguration();
        jobConf.set(JumboConstants.HOST, importHost.getHost());
        jobConf.setInt(JumboConstants.PORT, importHost.getPort());
        return new ControlledJob(job, new ArrayList<ControlledJob>());
    }

    public static List<ControlledJob> createDataImportJobs(Configuration conf, BaseJumboImportJob genericImportJob) throws IOException {
        List<ControlledJob> jobs = new ArrayList<ControlledJob>();
        for (ImportHost host : genericImportJob.getHosts()) {
            jobs.add(createJumboJob(conf, genericImportJob.getSortedInputPath(), new Path(genericImportJob.getLogOutputPath().toString() + "/" + host.getHost() + "_" + host.getPort() + "/data/"), JumboConstants.DATA_TYPE_DATA, genericImportJob, host, null));
        }
        return jobs;
    }

    public static List<ControlledJob> createIndexImportJobs(Configuration conf, BaseJumboImportJob genericImportJob, IndexField indexField) throws IOException {
        List<ControlledJob> jobs = new ArrayList<ControlledJob>();
        for (ImportHost host : genericImportJob.getHosts()) {
            jobs.add(createJumboJob(conf, new Path(genericImportJob.getIndexOutputPath().toString() + "/" + indexField.getIndexName() + "/"), new Path(genericImportJob.getLogOutputPath().toString() + "/" + host.getHost() + "_" + host.getPort() + "/index/" + indexField.getIndexName() + "/"), JumboConstants.DATA_TYPE_INDEX, genericImportJob, host, indexField));
        }
        return jobs;
    }
}
