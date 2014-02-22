package org.jumbodb.connector.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
public class JumboMetaUtil {
    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static void writeDeliveryMetaData(Path rootGenerationFolder, String info, Configuration conf) throws IOException {
        FileSystem fs = rootGenerationFolder.getFileSystem(conf);
        String delivery = conf.get(JumboConstants.DELIVERY_CHUNK_KEY);
        String version = conf.get(JumboConstants.DELIVERY_VERSION);
        Path suffix = rootGenerationFolder.suffix("/data/" + delivery + "/" + version + "/delivery.properties");
        if(!fs.exists(suffix)) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
            FSDataOutputStream fsDataOutputStream = fs.create(suffix, true);
            Properties indexInfo = new Properties();
            indexInfo.setProperty("date", sdf.format(new Date()));
            indexInfo.setProperty("delivery", delivery);
            indexInfo.setProperty("version", version);
            indexInfo.setProperty("info", info);
            indexInfo.store(fsDataOutputStream, "Delivery Information");
            IOUtils.closeStream(fsDataOutputStream);
        }
    }

    public static void writeIndexMetaData(Path indexFolder, String strategy, JobContext jobContext) throws IOException {
        FileSystem fs = indexFolder.getFileSystem(jobContext.getConfiguration());
        Path suffix = indexFolder.suffix("/index.properties");
        IndexField indexField = JumboConfigurationUtil.loadIndexJson(jobContext.getConfiguration());
        if(!fs.exists(suffix)) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
            FSDataOutputStream fsDataOutputStream = fs.create(suffix, true);
            Properties indexInfo = new Properties();
            indexInfo.setProperty("date", sdf.format(new Date()));
            indexInfo.setProperty("indexName", indexField.getIndexName());
            indexInfo.setProperty("strategy", strategy);
            indexInfo.setProperty("indexSourceFields", indexField.getFields().toString());
            indexInfo.store(fsDataOutputStream, "Index Information");
            IOUtils.closeStream(fsDataOutputStream);
        }
    }

    public static void writeCollectionMetaData(Path collectionFolder, String strategy, JobContext jobContext) throws IOException {
        FileSystem fs = collectionFolder.getFileSystem(jobContext.getConfiguration());
        Path suffix = collectionFolder.suffix("/collection.properties");
        if(!fs.exists(suffix)) {
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
            FSDataOutputStream fsDataOutputStream = fs.create(suffix, true);
            Properties deliveryInfo = new Properties();
            deliveryInfo.setProperty("sourcePath", getSourcePaths(jobContext));
            deliveryInfo.setProperty("date", sdf.format(new Date()));
            deliveryInfo.setProperty("strategy", strategy);
            deliveryInfo.store(fsDataOutputStream, "Delivery Information");
            IOUtils.closeStream(fsDataOutputStream);
        }
    }

    private static String getSourcePaths(JobContext context) {
        Path[] inputPaths = FileInputFormat.getInputPaths(context);
        StringBuilder buf = new StringBuilder();
        for (Path inputPath : inputPaths) {
            buf.append(inputPath.toString()).append(';');
        }
        return buf.toString();
    }
}
