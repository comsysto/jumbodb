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
import org.jumbodb.connector.hadoop.index.json.HostsJson;
import org.jumbodb.connector.hadoop.index.json.ImportJson;
import org.jumbodb.connector.hadoop.index.json.IndexJson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:21 PM
 */
public class ImportJobCreator {

    private static ControlledJob createJumboJob(Configuration conf, Path importPath, Path reportOutputPath, String type, ImportJson importJson, HostsJson hostsJson, IndexJson indexJson) throws IOException {
        String jobName = "jumboDB Import " + hostsJson.getHost() + " " + importPath.toString() + ":" + type;
        System.out.println(jobName);
        Job job = new Job(conf, jobName);
        JumboInputFormat.setDataType(job, type);
        JumboInputFormat.setImportPath(job, importPath);
        JumboInputFormat.setIndexName(job, indexJson != null ? indexJson.getIndexName() : "not_set");
        JumboInputFormat.setCollectionName(job, importJson.getCollectionName());
        JumboInputFormat.setDataStrategy(job, importJson.getDataStrategy());
        JumboInputFormat.setIndexStrategy(job, indexJson.getIndexStrategy());
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
        jobConf.set(JumboConstants.HOST, hostsJson.getHost());
        jobConf.setInt(JumboConstants.PORT, hostsJson.getPort());
        JumboJobCreator.sendMetaData(importJson, importPath, job.getConfiguration());
        JumboJobCreator.sendMetaIndex(importJson, indexJson, job.getConfiguration());
        return new ControlledJob(job, new ArrayList<ControlledJob>());
    }

    public static List<ControlledJob> createDataImportJobs(Configuration conf, Path importPathData, Path reportOutputPath, ImportJson importJson) throws IOException {
        List<ControlledJob> jobs = new ArrayList<ControlledJob>();
        for (HostsJson host : importJson.getHosts()) {
            jobs.add(createJumboJob(conf, importPathData, new Path(reportOutputPath.toString() + "/" + host.getHost() + "/data/"), JumboConstants.DATA_TYPE_DATA, importJson, host, null));
        }
        return jobs;
    }

    public static List<ControlledJob> createIndexImportJobs(Configuration conf, Path importPathIndex, Path reportOutputPath, ImportJson importJson, IndexJson indexJson) throws IOException {
        List<ControlledJob> jobs = new ArrayList<ControlledJob>();
        for (HostsJson host : importJson.getHosts()) {
            jobs.add(createJumboJob(conf, importPathIndex, new Path(reportOutputPath.toString() + "/" + host.getHost() + "/index/"), JumboConstants.DATA_TYPE_INDEX, importJson, host, indexJson));
        }
        return jobs;
    }
}
