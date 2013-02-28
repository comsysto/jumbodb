package org.jumbodb.connector.hadoop;

import org.jumbodb.connector.hadoop.importer.ImportJobCreator;
import org.jumbodb.connector.hadoop.index.IndexJobCreator;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.jumbodb.connector.importer.JumboImportConnection;
import org.jumbodb.connector.importer.MetaData;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * User: carsten
 * Date: 1/28/13
 * Time: 3:42 PM
 */
public class JumboJobCreator {

    public static List<ControlledJob> createIndexAndImportJob(Configuration conf, Path inputDataPath, Path outputIndexPath, Path outputReportPath, Class<? extends AbstractIndexMapper>... mapper) throws IOException {
        if(conf.get(JumboConstants.DELIVERY_VERSION) == null) {
            conf.set(JumboConstants.DELIVERY_VERSION, UUID.randomUUID().toString());
        }
        List<ControlledJob> controlledJobs = new LinkedList<ControlledJob>();

        ControlledJob dataImportJob = ImportJobCreator.createDataImportJob(conf, inputDataPath, new Path(outputReportPath.toString() + "/data"));
        controlledJobs.add(dataImportJob);

        for (Class<? extends AbstractIndexMapper> map : mapper) {
            IndexJobCreator.IndexControlledJob indexJob = IndexJobCreator.createIndexJob(conf, map, inputDataPath, outputIndexPath);
            ControlledJob controlledIndexJob = indexJob.getControlledJob();

            ControlledJob indexImportJob = ImportJobCreator.createIndexImportJob(conf, indexJob.getIndexPath(), new Path(outputReportPath.toString() + "/index"));
            indexImportJob.addDependingJob(controlledIndexJob);
            controlledJobs.add(controlledIndexJob);
            controlledJobs.add(indexImportJob);
        }
        return controlledJobs;
    }

    public static void sendMetaData(Configuration conf) {
        String type = conf.get(JumboConstants.DATA_TYPE);
        if(!JumboConstants.DATA_TYPE_DATA.equals(type)) {
            return;
        }
        JumboImportConnection jumbo = null;
        try {
            jumbo = new JumboImportConnection(conf.get(JumboConstants.HOST), conf.getInt(JumboConstants.PORT, JumboConstants.PORT_DEFAULT));
            String pathString = conf.get(JumboConstants.IMPORT_PATH);
            Path path = new Path(pathString);
            String collection = path.getName();
            boolean activate = conf.getBoolean(JumboConstants.DELIVERY_ACTIVATE, JumboConstants.DELIVERY_ACTIVATE_DEFAULT);
            MetaData metaData = new MetaData(collection, conf.get(JumboConstants.DELIVERY_KEY), conf.get(JumboConstants.DELIVERY_VERSION), pathString, activate, conf.get(JumboConstants.DELIVERY_INFO, "none"));
            jumbo.sendMetaData(metaData);
        } finally {
            IOUtils.closeStream(jumbo);
        }
    }

    public static void sendFinishedNotification(JobControl jobControl, Configuration conf) {
        if(jobControl.getFailedJobList().size() == 0
                && jobControl.allFinished()) {
            sendFinishedNotification(conf);
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
}
