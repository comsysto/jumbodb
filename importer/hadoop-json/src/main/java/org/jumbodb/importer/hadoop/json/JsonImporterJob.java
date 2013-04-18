package org.jumbodb.importer.hadoop.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jumbodb.connector.hadoop.HadoopConfigurationUtil;
import org.jumbodb.connector.hadoop.JumboConstants;
import org.jumbodb.connector.hadoop.JumboJobCreator;
import org.jumbodb.connector.hadoop.index.json.ImportJson;
import org.jumbodb.importer.hadoop.json.map.GenericJsonSortMapper;
import org.jumbodb.importer.hadoop.json.output.TextValueOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 2:32 PM
 */
public class JsonImporterJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new JsonImporterJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.printf("Usage: %s <json config file>\n", getClass().getSimpleName());
            return -1;
        }
        Configuration conf = getConf();
        String jsonConfPath = args[0];

        ImportJson importJson = HadoopConfigurationUtil.loadJsonConfigurationAndUpdateHadoop(jsonConfPath, conf);
        HadoopConfigurationUtil.updateHadoopConfiguration(conf, importJson);

        JobControl control = new JobControl("JsonImporterJob");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        String output = importJson.getOutput() + "/" + sdf.format(new Date()) + "/";
        String outputData = output + "data/";
        String outputIndex = output + "index/";
        String outputLog = output + "log/";

        String importDataPath = importJson.getInput();
        ControlledJob controlledSortJob = null;
        if(importJson.getSort() != null && importJson.getSort().size() > 0) {
            importDataPath = outputData;
            Job sortJob = new Job(conf, "Sort Job " + importJson.getCollectionName());
            FileInputFormat.addInputPath(sortJob, new Path(importJson.getInput()));
            FileOutputFormat.setOutputPath(sortJob, new Path(outputData));
            sortJob.setJarByClass(JsonImporterJob.class);
            sortJob.setMapperClass(GenericJsonSortMapper.class);
            sortJob.setMapOutputKeyClass(Text.class);
            sortJob.setMapOutputValueClass(Text.class);
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(Text.class);
            sortJob.setNumReduceTasks(importJson.getNumberOfOutputFiles());
            sortJob.setOutputFormatClass(TextValueOutputFormat.class);
            controlledSortJob = new ControlledJob(sortJob, Collections.<ControlledJob>emptyList());
            control.addJob(controlledSortJob);
        }

        if(conf.getBoolean(JumboConstants.EXPORT_ENABLED, true)) {
            String collectionName = importJson.getCollectionName();
            Path indexOutputPath = new Path(outputIndex + collectionName);
            Path logOutputPath = new Path(outputLog + collectionName);
            List<ControlledJob> jumboIndexAndImportJob = JumboJobCreator.createIndexAndImportJob(conf, new Path(importDataPath), indexOutputPath, logOutputPath, importJson);
            if(controlledSortJob != null) {
                for (ControlledJob current : jumboIndexAndImportJob) {
                    current.addDependingJob(controlledSortJob);
                }
            }
            control.addJobCollection(jumboIndexAndImportJob);
        }
        // index job, import job


        Thread t = new Thread(control);
        t.start();

        while (!control.allFinished()) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                System.err.println(e);
            }
        }
        JumboJobCreator.sendFinishedNotification(control, conf);
        return control.getFailedJobList().size() == 0 ? 0 : 1;
    }
}
