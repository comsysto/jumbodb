package org.jumbodb.connector.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.configuration.*;
import org.jumbodb.connector.hadoop.index.map.*;
import org.jumbodb.connector.hadoop.index.strategy.doubleval.snappy.GenericJsonDoubleIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.hashcode32.snappy.GenericJsonHashCode32IndexMapper;

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

    public static void setSortDatePatternConfig(Job conf, String sort) throws IOException {
        conf.getConfiguration().set(JumboConstants.JUMBO_SORT_DATEPATTERN_CONFIG, sort);
    }

    public static String loadSortDatePatternConfig(Configuration conf) throws IOException {
        String importJson = conf.get(JumboConstants.JUMBO_SORT_DATEPATTERN_CONFIG);
        if(StringUtils.isBlank(importJson)) {
            throw new IllegalStateException(JumboConstants.JUMBO_SORT_DATEPATTERN_CONFIG + " is not set.");
        }
        return importJson;
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
        String importJson = conf.get(GenericJsonHashCode32IndexMapper.JUMBO_INDEX_JSON_CONF);
        if(StringUtils.isBlank(importJson)) {
            throw new IllegalStateException(GenericJsonHashCode32IndexMapper.JUMBO_INDEX_JSON_CONF + " is not set.");
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
            for (IndexField indexField : job.getIndexes()) {
                if(indexField.getDatePattern() == null) {
                    indexField.setDatePattern(importDefinition.getDatePattern());
                }
            }

            String output = StringUtils.isNotBlank(importCollection.getOutput()) ? importCollection.getOutput() : importDefinition.getOutput();
            String outputWithDate = output + "/" + dateStamp + "/";
            String outputData = outputWithDate + "data/" + importCollection.getCollectionName() + "/";
            String outputIndex = outputWithDate + "index/" + importCollection.getCollectionName() + "/";
            String outputLog = outputWithDate + "log/" + importCollection.getCollectionName() + "/";
            job.setSortDatePattern(importCollection.getSortDatePattern() != null ? importCollection.getSortDatePattern() : importDefinition.getDatePattern());
            job.setSortType(importCollection.getSortType());
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

    public static Class<? extends Mapper> getSortMapperByType(String type) {
        Map<String, Class<? extends Mapper>> sortMapper = new HashMap<String, Class<? extends Mapper>>();
        sortMapper.put(GenericJsonStringSortMapper.SORT_KEY, GenericJsonStringSortMapper.class);
        sortMapper.put(GenericJsonDateTimeSortMapper.SORT_KEY, GenericJsonDateTimeSortMapper.class);
        sortMapper.put(GenericJsonDoubleSortMapper.SORT_KEY, GenericJsonDoubleSortMapper.class);
        sortMapper.put(GenericJsonFloatSortMapper.SORT_KEY, GenericJsonFloatSortMapper.class);
        sortMapper.put(GenericJsonIntegerSortMapper.SORT_KEY, GenericJsonIntegerSortMapper.class);
        sortMapper.put(GenericJsonLongSortMapper.SORT_KEY, GenericJsonLongSortMapper.class);
        Class<? extends Mapper> aClass = sortMapper.get(type);
        if(aClass != null) {
            return aClass;
        }
        throw new IllegalArgumentException("Sort type " + type + " is not supported.");
    }

    public static Class<? extends WritableComparable> getSortOutputKeyClassByType(String type) {
        Map<String, Class<? extends WritableComparable>> sortMapper = new HashMap<String, Class<? extends WritableComparable>>();
        sortMapper.put(GenericJsonStringSortMapper.SORT_KEY, Text.class);
        sortMapper.put(GenericJsonDateTimeSortMapper.SORT_KEY, LongWritable.class);
        sortMapper.put(GenericJsonDoubleSortMapper.SORT_KEY, DoubleWritable.class);
        sortMapper.put(GenericJsonFloatSortMapper.SORT_KEY, FloatWritable.class);
        sortMapper.put(GenericJsonIntegerSortMapper.SORT_KEY, IntWritable.class);
        sortMapper.put(GenericJsonLongSortMapper.SORT_KEY, LongWritable.class);
        Class<? extends WritableComparable> aClass = sortMapper.get(type);
        if(aClass != null) {
            return aClass;
        }
        throw new IllegalArgumentException("Sort type " + type + " is not supported.");
    }
}
