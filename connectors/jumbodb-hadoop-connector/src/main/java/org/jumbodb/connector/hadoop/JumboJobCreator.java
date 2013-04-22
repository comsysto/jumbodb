package org.jumbodb.connector.hadoop;

import org.jumbodb.connector.hadoop.importer.ImportJobCreator;
import org.jumbodb.connector.hadoop.index.IndexJobCreator;
import org.jumbodb.connector.hadoop.index.json.HostsJson;
import org.jumbodb.connector.hadoop.index.json.ImportJson;
import org.jumbodb.connector.hadoop.index.json.IndexJson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.UUID;

/**
 * User: carsten
 * Date: 1/28/13
 * Time: 3:42 PM
 */
public class JumboJobCreator {


    public static List<ControlledJob> createIndexAndImportJob(Configuration conf, Path inputDataPath, Path outputIndexPath, Path outputReportPath, ImportJson importJson) throws IOException {
        if(conf.get(JumboConstants.DELIVERY_KEY) == null) {
            conf.set(JumboConstants.DELIVERY_KEY, importJson.getDeliveryChunk());
        }
        if(conf.get(JumboConstants.DELIVERY_VERSION) == null) {
            conf.set(JumboConstants.DELIVERY_VERSION, UUID.randomUUID().toString());
        }
        List<ControlledJob> controlledJobs = new LinkedList<ControlledJob>();

        List<ControlledJob> dataImportJobs = ImportJobCreator.createDataImportJobs(conf, inputDataPath, outputReportPath, importJson);
        System.out.println("Number of dataImportJobs " + dataImportJobs.size());
        controlledJobs.addAll(dataImportJobs);

        for (IndexJson indexJson : importJson.getIndexes()) {
            IndexJobCreator.IndexControlledJob indexJob = IndexJobCreator.createGenericIndexJob(conf, indexJson, inputDataPath, outputIndexPath);
            ControlledJob controlledIndexJob = indexJob.getControlledJob();
            List<ControlledJob> indexImportJobs = ImportJobCreator.createIndexImportJobs(conf, indexJob.getIndexPath(), outputReportPath, importJson, indexJson);

            for (ControlledJob controlledJob : indexImportJobs) {
                controlledJob.addDependingJob(controlledIndexJob);
            }
            controlledJobs.add(controlledIndexJob);
            controlledJobs.addAll(indexImportJobs);
        }
        return controlledJobs;
    }

    public static List<ControlledJob> createIndexAndImportJob(Configuration conf, Path inputDataPath, Path outputIndexPath, Path outputReportPath, ImportJson importJson, Class<? extends AbstractIndexMapper>... mapper) throws IOException {
        if(conf.get(JumboConstants.DELIVERY_VERSION) == null) {
            conf.set(JumboConstants.DELIVERY_VERSION, UUID.randomUUID().toString());
        }
        List<ControlledJob> controlledJobs = new LinkedList<ControlledJob>();

        List<ControlledJob> dataImportJob = ImportJobCreator.createDataImportJobs(conf, inputDataPath, new Path(outputReportPath.toString() + "/data"), importJson);
        controlledJobs.addAll(dataImportJob);

        for (Class<? extends AbstractIndexMapper> map : mapper) {
            IndexJobCreator.IndexControlledJob indexJob = IndexJobCreator.createIndexJob(conf, map, inputDataPath, outputIndexPath);
            ControlledJob controlledIndexJob = indexJob.getControlledJob();

            IndexJson indexInformation = IndexJobCreator.getIndexInformation(map);
            List<ControlledJob> indexImportJob = ImportJobCreator.createIndexImportJobs(conf, indexJob.getIndexPath(), new Path(outputReportPath.toString() + "/index"), importJson, indexInformation);
            for (ControlledJob controlledJob : indexImportJob) {
                controlledJob.addDependingJob(controlledIndexJob);
            }
            controlledJobs.add(controlledIndexJob);
            controlledJobs.addAll(indexImportJob);
        }
        return controlledJobs;
    }

    public static void sendMetaIndex(ImportJson importJson, IndexJson indexJson, Configuration conf) {

        String type = conf.get(JumboConstants.DATA_TYPE);
        if(!JumboConstants.DATA_TYPE_INDEX.equals(type)) {
            return;
        }
        for (HostsJson hostsJson : importJson.getHosts()) {
            JumboImportConnection jumbo = null;
            try {
                jumbo = new JumboImportConnection(hostsJson.getHost(), hostsJson.getPort());
                String collection = importJson.getCollectionName();
                MetaIndex metaData = new MetaIndex(collection, importJson.getDeliveryChunk(), conf.get(JumboConstants.DELIVERY_VERSION), indexJson.getIndexName(), indexJson.getIndexStrategy());
                jumbo.sendMetaIndex(metaData);
            } finally {
                IOUtils.closeStream(jumbo);
            }
        }

    }

//    /**
//     * @param conf
//     */
//    @Deprecated
//    public static void sendMetaIndex(Configuration conf) {
//        String type = conf.get(JumboConstants.DATA_TYPE);
//        if(!JumboConstants.DATA_TYPE_INDEX.equals(type)) {
//            return;
//        }
//        JumboImportConnection jumbo = null;
//        try {
//            jumbo = new JumboImportConnection(conf.get(JumboConstants.HOST), conf.getInt(JumboConstants.PORT, JumboConstants.PORT_DEFAULT));
//            String pathString = conf.get(JumboConstants.IMPORT_PATH);
//            Path path = new Path(pathString);
//            String collection = path.getParent().getName();
//            String indexName = path.getName();
//            MetaIndex metaIndex = new MetaIndex(collection, conf.get(JumboConstants.DELIVERY_KEY), conf.get(JumboConstants.DELIVERY_VERSION), indexName, IndexJobCreator.HASHCODE_SNAPPY_V_1);
//            jumbo.sendMetaIndex(metaIndex);
//        } finally {
//            IOUtils.closeStream(jumbo);
//        }
//    }


    public static void sendMetaData(ImportJson importJson, Path importPath, Configuration conf) {
        String type = conf.get(JumboConstants.DATA_TYPE);
        if(!JumboConstants.DATA_TYPE_DATA.equals(type)) {
            return;
        }
        for (HostsJson hostsJson : importJson.getHosts()) {
            JumboImportConnection jumbo = null;
            try {
                jumbo = new JumboImportConnection(hostsJson.getHost(), hostsJson.getPort());
//                String collection = importPath.getName();
                String collection = importJson.getCollectionName();
                boolean activate = importJson.isActivateDelivery();
                MetaData metaData = new MetaData(collection, importJson.getDeliveryChunk(), conf.get(JumboConstants.DELIVERY_VERSION), importJson.getDataStrategy(),importPath.toString(), activate, importJson.getDescription());
                jumbo.sendMetaData(metaData);
            } finally {
                IOUtils.closeStream(jumbo);
            }
        }

    }

//    /**
//     * @param conf
//     */
//    @Deprecated
//    public static void sendMetaData(Configuration conf) {
//        String type = conf.get(JumboConstants.DATA_TYPE);
//        if(!JumboConstants.DATA_TYPE_DATA.equals(type)) {
//            return;
//        }
//        JumboImportConnection jumbo = null;
//        try {
//            jumbo = new JumboImportConnection(conf.get(JumboConstants.HOST), conf.getInt(JumboConstants.PORT, JumboConstants.PORT_DEFAULT));
//            String pathString = conf.get(JumboConstants.IMPORT_PATH);
//            Path path = new Path(pathString);
//            String collection = path.getName();
//            boolean activate = conf.getBoolean(JumboConstants.DELIVERY_ACTIVATE, JumboConstants.DELIVERY_ACTIVATE_DEFAULT);
//            MetaData metaData = new MetaData(collection, conf.get(JumboConstants.DELIVERY_KEY), conf.get(JumboConstants.DELIVERY_VERSION), pathString, activate, conf.get(JumboConstants.DELIVERY_INFO, "none"));
//            jumbo.sendMetaData(metaData);
//        } finally {
//            IOUtils.closeStream(jumbo);
//        }
//    }

    public static void sendFinishedNotification(JobControl jobControl, Configuration conf) {
        if(!conf.getBoolean(JumboConstants.EXPORT_ENABLED, false)) {
            return;
        }
        if(jobControl.getFailedJobList().size() == 0
                && jobControl.allFinished()) {
            sendFinishedNotification(conf);
        }

    }

    public static void sendFinishedNotification(ImportJson importJson, JobControl jobControl, Configuration conf) {
        if(!conf.getBoolean(JumboConstants.EXPORT_ENABLED, true)) {
            return;
        }
        if(jobControl.getFailedJobList().size() == 0
                && jobControl.allFinished()) {
            sendFinishedNotification(importJson, conf);
        }

    }

    public static void sendFinishedNotification(Configuration conf) {
        JumboImportConnection jumbo = null;
        try {
            jumbo = new JumboImportConnection(conf.get(JumboConstants.HOST), conf.getInt(JumboConstants.PORT, JumboConstants.PORT_DEFAULT));
            jumbo.sendFinishedNotification(conf.get(JumboConstants.DELIVERY_KEY), conf.get(JumboConstants.DELIVERY_VERSION));
        } finally {
            IOUtils.closeStream(jumbo);
        }
    }

    public static void sendFinishedNotification(ImportJson importJson, Configuration conf) {
        String deliveryChunk = importJson.getDeliveryChunk();
        String deliveryVersion = conf.get(JumboConstants.DELIVERY_VERSION);
        for (HostsJson hostsJson : importJson.getHosts()) {
            JumboImportConnection jumbo = null;
            try {
                jumbo = new JumboImportConnection(hostsJson.getHost(), hostsJson.getPort());
                jumbo.sendFinishedNotification(deliveryChunk, deliveryVersion);
            } finally {
                IOUtils.closeStream(jumbo);
            }
        }

    }
}
