package org.jumbodb.connector.hadoop;

import org.apache.commons.lang.StringUtils;
import org.jumbodb.connector.hadoop.importer.ImportJobCreator;
import org.jumbodb.connector.hadoop.index.IndexJobCreator;
import org.jumbodb.connector.hadoop.configuration.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;
import org.jumbodb.connector.importer.JumboImportConnection;
import org.jumbodb.connector.importer.MetaData;
import org.jumbodb.connector.importer.MetaIndex;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * User: carsten
 * Date: 1/28/13
 * Time: 3:42 PM
 */
public class JumboJobCreator {


    public static List<ControlledJob> createIndexAndImportJob(Configuration conf, JumboGenericImportJob genericImportJob) throws IOException {
        if(conf.get(JumboConstants.DELIVERY_VERSION) == null) {
            conf.set(JumboConstants.DELIVERY_VERSION, UUID.randomUUID().toString());
        }
        List<ControlledJob> controlledJobs = new LinkedList<ControlledJob>();

        List<ControlledJob> dataImportJobs = ImportJobCreator.createDataImportJobs(conf, genericImportJob);
        System.out.println("Number of dataImportJobs " + dataImportJobs.size());
        controlledJobs.addAll(dataImportJobs);

        for (IndexField indexField : genericImportJob.getIndexes()) {
            IndexJobCreator.IndexControlledJob indexJob = IndexJobCreator.createGenericIndexJob(conf, genericImportJob, indexField);
            ControlledJob controlledIndexJob = indexJob.getControlledJob();
            List<ControlledJob> indexImportJobs = ImportJobCreator.createIndexImportJobs(conf, genericImportJob, indexField);

            for (ControlledJob controlledJob : indexImportJobs) {
                controlledJob.addDependingJob(controlledIndexJob);
            }
            controlledJobs.add(controlledIndexJob);
            controlledJobs.addAll(indexImportJobs);
        }
        return controlledJobs;
    }

    public static List<ControlledJob> createIndexAndImportJob(Configuration conf, JumboCustomImportJob jumboCustomImportJob) throws IOException {
        if(conf.get(JumboConstants.DELIVERY_VERSION) == null) {
            conf.set(JumboConstants.DELIVERY_VERSION, UUID.randomUUID().toString());
        }
        List<ControlledJob> controlledJobs = new LinkedList<ControlledJob>();

        List<ControlledJob> dataImportJob = ImportJobCreator.createDataImportJobs(conf, jumboCustomImportJob);
        controlledJobs.addAll(dataImportJob);

        for (Class<? extends AbstractIndexMapper> map : jumboCustomImportJob.getMapper()) {
            IndexJobCreator.IndexControlledJob indexJob = IndexJobCreator.createCustomIndexJob(conf, jumboCustomImportJob, map);
            ControlledJob controlledIndexJob = indexJob.getControlledJob();

            IndexField indexInformation = IndexJobCreator.getIndexInformation(map);
            List<ControlledJob> indexImportJob = ImportJobCreator.createIndexImportJobs(conf, jumboCustomImportJob, indexInformation);
            for (ControlledJob controlledJob : indexImportJob) {
                controlledJob.addDependingJob(controlledIndexJob);
            }
            controlledJobs.add(controlledIndexJob);
            controlledJobs.addAll(indexImportJob);
        }
        return controlledJobs;
    }

    public static void sendMetaIndex(BaseJumboImportJob genericImportJob, IndexField indexField, Configuration conf) {

        String type = conf.get(JumboConstants.DATA_TYPE);
        if(!JumboConstants.DATA_TYPE_INDEX.equals(type)) {
            return;
        }
        for (ImportHost importHost : genericImportJob.getHosts()) {
            JumboImportConnection jumbo = null;
            try {
                jumbo = new JumboImportConnection(importHost.getHost(), importHost.getPort());
                String collection = genericImportJob.getCollectionName();
                MetaIndex metaData = new MetaIndex(collection, genericImportJob.getDeliveryChunkKey(), conf.get(JumboConstants.DELIVERY_VERSION), indexField.getIndexName(), indexField.getIndexStrategy(), StringUtils.join(indexField.getFields(), ";"));
                jumbo.sendMetaIndex(metaData);
            } finally {
                IOUtils.closeStream(jumbo);
            }
        }

    }


    public static void sendMetaData(BaseJumboImportJob genericImportJob, Configuration conf) {
        String type = conf.get(JumboConstants.DATA_TYPE);
        if(!JumboConstants.DATA_TYPE_DATA.equals(type)) {
            return;
        }
        for (ImportHost importHost : genericImportJob.getHosts()) {
            JumboImportConnection jumbo = null;
            try {
                jumbo = new JumboImportConnection(importHost.getHost(), importHost.getPort());
                String collection = genericImportJob.getCollectionName();
                boolean activate = genericImportJob.isActivateDelivery();
                MetaData metaData = new MetaData(collection, genericImportJob.getDeliveryChunkKey(), conf.get(JumboConstants.DELIVERY_VERSION), genericImportJob.getDataStrategy(),genericImportJob.getInputPath().toString(), activate, genericImportJob.getDescription());
                jumbo.sendMetaData(metaData);
            } finally {
                IOUtils.closeStream(jumbo);
            }
        }

    }

    public static void sendFinishedNotification(Set<FinishedNotification> finishedNotifications, JobControl jobControl, Configuration conf) {
        for (FinishedNotification finishedNotification : finishedNotifications) {
            sendFinishedNotification(finishedNotification, jobControl, conf);
        }
    }


    public static void sendFinishedNotification(Set<FinishedNotification> finishedNotifications, Configuration conf) {
        for (FinishedNotification finishedNotification : finishedNotifications) {
            sendFinishedNotification(finishedNotification, conf);
        }
    }

    public static void sendFinishedNotification(FinishedNotification finishedNotification, JobControl jobControl, Configuration conf) {
        if(!conf.getBoolean(JumboConstants.EXPORT_ENABLED, true)) {
            return;
        }
        if(jobControl.getFailedJobList().size() == 0
                && jobControl.allFinished()) {
            sendFinishedNotification(finishedNotification, conf);
        }

    }


    public static void sendFinishedNotification(FinishedNotification finishedNotification, Configuration conf) {
        String deliveryVersion = conf.get(JumboConstants.DELIVERY_VERSION);
        JumboImportConnection jumbo = null;
        try {
            ImportHost host = finishedNotification.getHost();
            jumbo = new JumboImportConnection(host.getHost(), host.getPort());
            jumbo.sendFinishedNotification(finishedNotification.getDeliveryChunkKey(), deliveryVersion);
        } finally {
            IOUtils.closeStream(jumbo);
        }
    }
}
