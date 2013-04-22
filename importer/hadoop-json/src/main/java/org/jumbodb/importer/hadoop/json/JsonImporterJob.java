package org.jumbodb.importer.hadoop.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.JumboConstants;
import org.jumbodb.connector.hadoop.JumboJobCreator;
import org.jumbodb.connector.hadoop.configuration.FinishedNotification;
import org.jumbodb.connector.hadoop.configuration.ImportDefinition;
import org.jumbodb.connector.hadoop.configuration.JumboGenericImportJob;
import org.jumbodb.connector.hadoop.index.map.GenericJsonSortMapper;
import org.jumbodb.connector.hadoop.index.output.TextValueOutputFormat;

import java.util.Collections;
import java.util.List;
import java.util.Set;

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
            System.err.printf("Usage: %s <configuration config file>\n", getClass().getSimpleName());
            return -1;
        }
        Configuration conf = getConf();
        String jsonConfPath = args[0];

        ImportDefinition importDefinition = JumboConfigurationUtil.loadJsonConfigurationAndUpdateHadoop(jsonConfPath, conf);
        List<JumboGenericImportJob> importJobs = JumboConfigurationUtil.convertToGenericImportJobs(importDefinition);

        JobControl control = new JobControl("JsonImporterJob");

        for (JumboGenericImportJob importJob : importJobs) {
            ControlledJob controlledSortJob = null;
            if(importJob.getSort() != null && importJob.getSort().size() > 0) {
                Job sortJob = new Job(conf, "Sort Job " + importJob.getCollectionName());
                JumboConfigurationUtil.setSortConfig(sortJob, importJob.getSort());
                FileInputFormat.addInputPath(sortJob, importJob.getInputPath());
                FileOutputFormat.setOutputPath(sortJob, importJob.getSortedOutputPath());
                sortJob.setJarByClass(JsonImporterJob.class);
                sortJob.setMapperClass(GenericJsonSortMapper.class);
                sortJob.setMapOutputKeyClass(Text.class);
                sortJob.setMapOutputValueClass(Text.class);
                sortJob.setOutputKeyClass(Text.class);
                sortJob.setOutputValueClass(Text.class);
                sortJob.setNumReduceTasks(importJob.getNumberOfOutputFiles());
                sortJob.setOutputFormatClass(TextValueOutputFormat.class);
                controlledSortJob = new ControlledJob(sortJob, Collections.<ControlledJob>emptyList());
                control.addJob(controlledSortJob);
            }

            if(conf.getBoolean(JumboConstants.EXPORT_ENABLED, true)) {
                List<ControlledJob> jumboIndexAndImportJob = JumboJobCreator.createIndexAndImportJob(conf,  importJob);
                System.out.println("Number of Jumbo Index and Import Jobs " + jumboIndexAndImportJob.size());
                if(controlledSortJob != null) {
                    for (ControlledJob current : jumboIndexAndImportJob) {
                        current.addDependingJob(controlledSortJob);
                    }
                }
                control.addJobCollection(jumboIndexAndImportJob);
                System.out.println("Number of Jumbo Index and Import JobsWaiting Jobs " + control.getWaitingJobList().size());
            }

        }

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
        Set<FinishedNotification> finishedNotifications = JumboConfigurationUtil.convertToFinishedNotifications(importDefinition);
        JumboJobCreator.sendFinishedNotification(finishedNotifications, control, conf);
        return control.getFailedJobList().size() == 0 ? 0 : 1;
    }
}
