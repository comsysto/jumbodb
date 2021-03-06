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
import org.apache.hadoop.mapreduce.lib.partition.BinaryPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.JumboJobCreator;
import org.jumbodb.connector.hadoop.JumboMetaUtil;
import org.jumbodb.connector.hadoop.configuration.CommitNotification;
import org.jumbodb.connector.hadoop.configuration.DataStrategies;
import org.jumbodb.connector.hadoop.configuration.DataStrategy;
import org.jumbodb.connector.hadoop.configuration.ImportDefinition;
import org.jumbodb.connector.hadoop.configuration.JumboGenericImportJob;
import org.jumbodb.connector.hadoop.importer.input.JumboInputFormat;
import org.jumbodb.connector.hadoop.index.output.data.SnappyDataV1OutputFormat;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Starter for the JSON import Job
 *
 * @author Carsten Hufe
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
            System.err.printf("Usage: %s <configuration config file>%n", getClass().getSimpleName());
            return -1;
        }
        Configuration conf = getConf();
        String jsonConfPath = args[0];

        ImportDefinition importDefinition = JumboConfigurationUtil.loadJsonConfigurationAndUpdateHadoop(jsonConfPath, conf);
        String outputWithDate = JumboConfigurationUtil.getOutputPathWithDateStamp(importDefinition);
        List<JumboGenericImportJob> importJobs = JumboConfigurationUtil.convertToGenericImportJobs(conf, importDefinition, outputWithDate);
        JobControl control = new JobControl("JsonImporterJob");
        for (JumboGenericImportJob importJob : importJobs) {
            Job sortJob = Job.getInstance(conf, "Sort Job " + importJob.getCollectionName());
            JumboConfigurationUtil.setSortConfig(sortJob, importJob.getSort());
            JumboConfigurationUtil.setSortDatePatternConfig(sortJob, importJob.getSortDatePattern());
            JumboConfigurationUtil.setCollectionInfo(sortJob, importJob.getDescription());
            JumboInputFormat.setChecksumType(sortJob, importDefinition.getChecksum());
            FileInputFormat.addInputPath(sortJob, importJob.getInputPath());
            FileOutputFormat.setOutputPath(sortJob, importJob.getSortedOutputPath());
            sortJob.setJarByClass(JsonImporterJob.class);
            final DataStrategy dataStrategy = DataStrategies.getDataStrategy(importJob.getDataStrategy());
            sortJob.setMapperClass(dataStrategy.getSortMapperByType(importJob.getSortType()));
            sortJob.setMapOutputKeyClass(dataStrategy.getSortOutputKeyClassByType(importJob.getSortType()));
            sortJob.setMapOutputValueClass(dataStrategy.getOutputClass());
            sortJob.setOutputKeyClass(dataStrategy.getSortOutputKeyClassByType(importJob.getSortType()));
            sortJob.setOutputValueClass(dataStrategy.getOutputClass());
            sortJob.setNumReduceTasks(importJob.getNumberOfOutputFiles());
            sortJob.setOutputFormatClass(dataStrategy.getOutputFormat());
            sortJob.setPartitionerClass(HashPartitioner.class);
            ControlledJob controlledSortJob = new ControlledJob(sortJob, Collections.<ControlledJob>emptyList());
            control.addJob(controlledSortJob);

            List<ControlledJob> jumboIndexAndImportJob = JumboJobCreator.createIndexAndImportJob(conf, importJob);
            System.out.println("Number of Jumbo Index and Import Jobs " + jumboIndexAndImportJob.size());
            for (ControlledJob current : jumboIndexAndImportJob) {
                current.addDependingJob(controlledSortJob);
            }
            control.addJobCollection(jumboIndexAndImportJob);
            System.out.println("Waiting Jobs " + control.getWaitingJobList().size());
        }
        JumboMetaUtil.writeDeliveryMetaData(new Path(outputWithDate), importDefinition.getDescription(), conf);
        JumboMetaUtil.writeActiveMetaData(new Path(outputWithDate), conf);
        JumboJobCreator.initImport(importDefinition.getHosts(), importDefinition.getDeliveryChunkKey(), importDefinition.getDescription(), conf);

        Thread t = new Thread(control);
        t.start();

        while (!control.allFinished()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.err.println(e);
            }
        }
        Set<CommitNotification> commitNotifications = JumboConfigurationUtil.convertToImportCommits(importDefinition);
        JumboJobCreator.commitImport(commitNotifications, control, conf);
        return control.getFailedJobList().size() == 0 ? 0 : 1;
    }

}
