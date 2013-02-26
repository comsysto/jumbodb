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

import java.io.IOException;
import java.util.ArrayList;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:21 PM
 */
public class ImportJobCreator {

    public static ControlledJob createIndexImportJob(Configuration conf, Path importPathIndex, Path reportOutputPath) throws IOException {
        return createJumboJob(conf, importPathIndex, reportOutputPath, JumboConstants.DATA_TYPE_INDEX);
    }

    public static ControlledJob createDataImportJob(Configuration conf, Path importPathData, Path reportOutputPath) throws IOException {
        return createJumboJob(conf, importPathData, reportOutputPath, JumboConstants.DATA_TYPE_DATA);
    }

    private static ControlledJob createJumboJob(Configuration conf, Path importPath, Path reportOutputPath, String type) throws IOException {
        Job job = new Job(conf, "jumboDB Import Job " + importPath.toString() + ":" + type);
        JumboInputFormat.setDataType(job, type);
        JumboInputFormat.setImportPath(job, importPath);
        JumboInputFormat.setDataType(job, type);
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
        JumboJobCreator.sendMetaData(job.getConfiguration());
        return new ControlledJob(job, new ArrayList<ControlledJob>());
    }
}
