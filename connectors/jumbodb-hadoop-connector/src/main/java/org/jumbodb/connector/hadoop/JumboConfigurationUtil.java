package org.jumbodb.connector.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.configuration.*;
import org.jumbodb.connector.hadoop.index.strategy.hashcode.snappy.GenericJsonHashCodeIndexMapper;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: carsten
 * Date: 11/2/12
 * Time: 1:22 PM
 */
public class JumboConfigurationUtil {

    public static void loadConfigurationFileToHadoop(Configuration conf, String configFileLocation) throws IOException {
        Path configFilePath = new Path(configFileLocation);
        FileSystem fs = FileSystem.get(URI.create(configFileLocation), conf);

        FSDataInputStream fdis = fs.open(configFilePath);
        Properties props = new Properties();
        props.load(fdis);
        System.out.println("Loaded Properties:\n" + props.toString());

        // set the properties into Config object so it is available for Mappers and Reducers
        Enumeration keys = props.propertyNames();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            String value = props.getProperty(key);
            conf.set(key, value);
        }
        fdis.close();
    }


    public static ImportDefinition loadJsonConfigurationAndUpdateHadoop(String configFileLocation, Configuration conf) throws IOException {
        FSDataInputStream fdis = null;
        try {
            Path configFilePath = new Path(configFileLocation);
            FileSystem fs = FileSystem.get(URI.create(configFileLocation), conf);
            fdis = fs.open(configFilePath);
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
            ImportDefinition importDefinition = mapper.readValue(fdis, ImportDefinition.class);
            if(conf.get(JumboConstants.DELIVERY_CHUNK_KEY) == null) {
                conf.set(JumboConstants.DELIVERY_CHUNK_KEY, importDefinition.getDeliveryChunkKey());
            }
            if(conf.get(JumboConstants.DELIVERY_VERSION) == null) {
                conf.set(JumboConstants.DELIVERY_VERSION, UUID.randomUUID().toString());
            }
            updateHadoopConfiguration(conf, importDefinition.getHadoop());
            return importDefinition;

        } finally {
            IOUtils.closeQuietly(fdis);
        }
    }

    public static void setSortConfig(Job conf, List<String> sort) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        conf.getConfiguration().set(JumboConstants.JUMBO_SORT_CONFIG, mapper.writeValueAsString(sort));
    }

    public static List<String> loadSortConfig(Configuration conf) throws IOException {
        String importJson = conf.get(JumboConstants.JUMBO_SORT_CONFIG);
        if(StringUtils.isBlank(importJson)) {
            throw new IllegalStateException(JumboConstants.JUMBO_SORT_CONFIG + " is not set.");
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        return mapper.readValue(importJson, List.class);
    }

    public static IndexField loadIndexJson(Configuration conf) throws IOException {
        String importJson = conf.get(GenericJsonHashCodeIndexMapper.JUMBO_INDEX_JSON_CONF);
        if(StringUtils.isBlank(importJson)) {
            throw new IllegalStateException(GenericJsonHashCodeIndexMapper.JUMBO_INDEX_JSON_CONF + " is not set.");
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        return mapper.readValue(importJson, IndexField.class);
    }

    public static void updateHadoopConfiguration(Configuration conf, List<HadoopConfig> importCollection) {
        for (HadoopConfig config : importCollection) {
            conf.set(config.getKey(), config.getValue());
        }
    }

    public static List<JumboGenericImportJob> convertToGenericImportJobs(ImportDefinition importDefinition) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        String dateStamp = sdf.format(new Date());

        List<JumboGenericImportJob> result = new LinkedList<JumboGenericImportJob>();
        for (ImportCollection importCollection : importDefinition.getImportCollection()) {
            JumboGenericImportJob job = new JumboGenericImportJob();
            job.setIndexes(importCollection.getIndexes());
            job.setSort(importCollection.getSort());
            job.setActivateDelivery(importCollection.getActivateDelivery() != null ? importCollection.getActivateDelivery() : importDefinition.getActivateDelivery());
            job.setCollectionName(importCollection.getCollectionName());
            job.setDataStrategy(importCollection.getDataStrategy());
            job.setDeliveryChunkKey(importCollection.getDeliveryChunkKey() != null ? importCollection.getDeliveryChunkKey() : importDefinition.getDeliveryChunkKey());
            job.setDescription(importCollection.getDescription() != null ? importCollection.getDescription() : importDefinition.getDescription());
            job.setHosts(importCollection.getHosts() != null && importCollection.getHosts().size() > 0 ? importCollection.getHosts() : importDefinition.getHosts());

            String output = StringUtils.isNotBlank(importCollection.getOutput()) ? importCollection.getOutput() : importDefinition.getOutput();
            String outputWithDate = output + "/" + dateStamp + "/";
            String outputData = outputWithDate + "data/" + importCollection.getCollectionName() + "/";
            String outputIndex = outputWithDate + "index/" + importCollection.getCollectionName() + "/";
            String outputLog = outputWithDate + "log/" + importCollection.getCollectionName() + "/";

            job.setInputPath(new Path(importCollection.getInput()));
            job.setSortedOutputPath(importCollection.getSort() != null && importCollection.getSort().size() > 0 ? new Path(outputData) : null);
            job.setIndexOutputPath(new Path(outputIndex));
            job.setLogOutputPath(new Path(outputLog));
            job.setNumberOfOutputFiles(importCollection.getNumberOfOutputFiles() != null ? importCollection.getNumberOfOutputFiles() : importDefinition.getNumberOfOutputFiles());
            result.add(job);
        }
        return result;
    }

    public static Set<FinishedNotification> convertToFinishedNotifications(ImportDefinition importDefinition) {
        Set<FinishedNotification> result = new HashSet<FinishedNotification>();
        for (ImportCollection importCollection : importDefinition.getImportCollection()) {
            String deliveryChunkKey = importCollection.getDeliveryChunkKey() != null ? importCollection.getDeliveryChunkKey() : importDefinition.getDeliveryChunkKey();
            List<ImportHost> importHosts = importCollection.getHosts() != null && importCollection.getHosts().size() > 0 ? importCollection.getHosts() : importDefinition.getHosts();
            for (ImportHost importHost : importHosts) {
                FinishedNotification finishedNotification = new FinishedNotification(deliveryChunkKey, importHost);
                result.add(finishedNotification);
            }
        }
        return result;
    }
}
