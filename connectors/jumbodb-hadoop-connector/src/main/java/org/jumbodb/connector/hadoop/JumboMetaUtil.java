package org.jumbodb.connector.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.data.common.meta.DeliveryProperties;
import org.jumbodb.data.common.meta.IndexProperties;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
// CARSTEN reuse from common classes like DeliveryProperties
public class JumboMetaUtil {

    public static void writeActiveMetaData(Path rootGenerationFolder, Configuration conf) throws IOException {
        FileSystem fs = rootGenerationFolder.getFileSystem(conf);
        String delivery = conf.get(JumboConstants.DELIVERY_CHUNK_KEY);
        String version = conf.get(JumboConstants.DELIVERY_VERSION);
        Path suffix = rootGenerationFolder.suffix("/data/" + delivery + "/active.properties");
        if (!fs.exists(suffix)) {
            FSDataOutputStream fsDataOutputStream = fs.create(suffix, true);
            Properties activeInfo = new Properties();
            activeInfo.setProperty("active", "true");
            activeInfo.setProperty("version", version);
            activeInfo.store(fsDataOutputStream, "Active version");
            IOUtils.closeStream(fsDataOutputStream);
        }
    }

    public static void writeDeliveryMetaData(Path rootGenerationFolder, String info, Configuration conf) throws IOException {
        FileSystem fs = rootGenerationFolder.getFileSystem(conf);
        String delivery = conf.get(JumboConstants.DELIVERY_CHUNK_KEY);
        String version = conf.get(JumboConstants.DELIVERY_VERSION);
        Path suffix = rootGenerationFolder.suffix("/data/" + delivery + "/" + version + "/delivery.properties");
        if (!fs.exists(suffix)) {
            SimpleDateFormat sdf = new SimpleDateFormat(DeliveryProperties.DATE_PATTERN);
            FSDataOutputStream fsDataOutputStream = fs.create(suffix, true);
            Properties deliveryInfo = new Properties();
            deliveryInfo.setProperty("date", sdf.format(new Date()));
            deliveryInfo.setProperty("delivery", delivery);
            deliveryInfo.setProperty("version", version);
            deliveryInfo.setProperty("info", info);
            deliveryInfo.store(fsDataOutputStream, "Delivery Information");
            IOUtils.closeStream(fsDataOutputStream);
        }
    }

    public static void writeIndexMetaData(Path indexFolder, String strategy, JobContext jobContext) throws IOException {
        FileSystem fs = indexFolder.getFileSystem(jobContext.getConfiguration());
        Path suffix = indexFolder.suffix("/index.properties");
        IndexField indexField = JumboConfigurationUtil.loadIndexJson(jobContext.getConfiguration());
        if (!fs.exists(suffix)) {
            SimpleDateFormat sdf = new SimpleDateFormat(DeliveryProperties.DATE_PATTERN);
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
        Configuration configuration = jobContext.getConfiguration();
        FileSystem fs = collectionFolder.getFileSystem(configuration);
        Path suffix = collectionFolder.suffix("/collection.properties");
        if (!fs.exists(suffix)) {
            SimpleDateFormat sdf = new SimpleDateFormat(DeliveryProperties.DATE_PATTERN);
            FSDataOutputStream fsDataOutputStream = fs.create(suffix, true);
            Properties collectionInfo = new Properties();
            collectionInfo.setProperty("sourcePath", getSourcePaths(jobContext));
            collectionInfo.setProperty("date", sdf.format(new Date()));
            collectionInfo.setProperty("info", configuration.get(JumboConstants.JUMBO_COLLECTION_INFO));
            collectionInfo.setProperty("strategy", strategy);
            collectionInfo.setProperty("dateFormat", JumboConfigurationUtil.loadCollectionDateFormat(configuration));
            collectionInfo.store(fsDataOutputStream, "Delivery Information");
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
