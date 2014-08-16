package org.jumbodb.connector.hadoop;

import org.jumbodb.connector.hadoop.importer.ImportJobCreator;
import org.jumbodb.connector.hadoop.index.IndexJobCreator;
import org.jumbodb.connector.hadoop.configuration.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;
import org.jumbodb.connector.importer.ImportInfo;
import org.jumbodb.connector.importer.JumboImportConnection;
import org.jumbodb.data.common.meta.DeliveryProperties;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: carsten
 * Date: 1/28/13
 * Time: 3:42 PM
 */
public class JumboJobCreator {
    public static List<ControlledJob> createIndexAndImportJob(Configuration conf, JumboGenericImportJob genericImportJob) throws IOException {
        if (conf.get(JumboConstants.DELIVERY_VERSION) == null) {
            conf.set(JumboConstants.DELIVERY_VERSION, UUID.randomUUID().toString());
        }
        List<ControlledJob> controlledJobs = new LinkedList<ControlledJob>();

        List<ControlledJob> dataImportJobs = ImportJobCreator.createDataImportJobs(conf, genericImportJob);
        System.out.println("Number of dataImportJobs " + dataImportJobs.size());
        controlledJobs.addAll(dataImportJobs);

        for (IndexField indexField : genericImportJob.getIndexes()) {
            IndexJobCreator.IndexControlledJob indexJob = IndexJobCreator.createGenericIndexJob(conf, genericImportJob, indexField);
            ControlledJob controlledIndexJob = indexJob.getControlledJob();
            controlledJobs.add(controlledIndexJob);
            if (conf.getBoolean(JumboConstants.EXPORT_ENABLED, true)) {
                List<ControlledJob> indexImportJobs = ImportJobCreator.createIndexImportJobs(conf, genericImportJob, indexField);
                for (ControlledJob controlledJob : indexImportJobs) {
                    controlledJob.addDependingJob(controlledIndexJob);
                }
                controlledJobs.addAll(indexImportJobs);
            }
        }
        return controlledJobs;
    }

    public static void initImport(List<ImportHost> importHosts, String chunkKey, String deliveryDescription, Configuration conf) {
        String deliveryVersion = conf.get(JumboConstants.DELIVERY_VERSION);
        for (ImportHost host : importHosts) {
            JumboImportConnection jumbo = null;
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(DeliveryProperties.DATE_PATTERN);
                jumbo = new JumboImportConnection(host.getHost(), host.getPort());
                jumbo.initImport(new ImportInfo(chunkKey, deliveryVersion, sdf.format(new Date()), deliveryDescription));
            } finally {
                IOUtils.closeStream(jumbo);
            }
        }
    }

    public static List<ControlledJob> createIndexAndImportJob(Configuration conf, JumboCustomImportJob jumboCustomImportJob) throws IOException {
        if (conf.get(JumboConstants.DELIVERY_VERSION) == null) {
            conf.set(JumboConstants.DELIVERY_VERSION, UUID.randomUUID().toString());
        }
        List<ControlledJob> controlledJobs = new LinkedList<ControlledJob>();

        List<ControlledJob> dataImportJob = ImportJobCreator.createDataImportJobs(conf, jumboCustomImportJob);
        controlledJobs.addAll(dataImportJob);

        for (Class<? extends AbstractIndexMapper> map : jumboCustomImportJob.getMapper()) {
            IndexJobCreator.IndexControlledJob indexJob = IndexJobCreator.createCustomIndexJob(conf, jumboCustomImportJob, map);
            ControlledJob controlledIndexJob = indexJob.getControlledJob();
            controlledJobs.add(controlledIndexJob);
            if (conf.getBoolean(JumboConstants.EXPORT_ENABLED, true)) {


                IndexField indexInformation = IndexJobCreator.getIndexInformation(map);
                List<ControlledJob> indexImportJob = ImportJobCreator.createIndexImportJobs(conf, jumboCustomImportJob, indexInformation);
                for (ControlledJob controlledJob : indexImportJob) {
                    controlledJob.addDependingJob(controlledIndexJob);
                }
                controlledJobs.addAll(indexImportJob);

            }
        }
        return controlledJobs;
    }

    public static void sendFinishedNotification(Set<CommitNotification> commitNotifications, JobControl jobControl, Configuration conf) {
        for (CommitNotification commitNotification : commitNotifications) {
            sendFinishedNotification(commitNotification, jobControl, conf);
        }
    }


    public static void sendFinishedNotification(Set<CommitNotification> commitNotifications, Configuration conf) {
        for (CommitNotification commitNotification : commitNotifications) {
            commitImport(commitNotification, conf);
        }
    }

    public static void sendFinishedNotification(CommitNotification commitNotification, JobControl jobControl, Configuration conf) {
        if (!conf.getBoolean(JumboConstants.EXPORT_ENABLED, true)) {
            return;
        }
        if (jobControl.getFailedJobList().size() == 0
                && jobControl.allFinished()) {
            commitImport(commitNotification, conf);
        }

    }


    public static void commitImport(CommitNotification commitNotification, Configuration conf) {
        String deliveryVersion = conf.get(JumboConstants.DELIVERY_VERSION);
        JumboImportConnection jumbo = null;
        try {
            ImportHost host = commitNotification.getHost();
            jumbo = new JumboImportConnection(host.getHost(), host.getPort());
            jumbo.commitImport(commitNotification.getDeliveryChunkKey(), deliveryVersion, commitNotification.isActivateChunk(), commitNotification.isActivateVersion());
        } finally {
            IOUtils.closeStream(jumbo);
        }
    }
}
