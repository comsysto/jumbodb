package org.jumbodb.connector.hadoop;

import com.google.common.collect.Maps;
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
import org.jumbodb.common.query.ChecksumType;
import org.jumbodb.connector.hadoop.configuration.*;
import org.jumbodb.connector.hadoop.index.map.*;

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
    public static final String JUMBO_INDEX_JSON_CONF = "jumbo.index.configuration";

    private static final Map<String, Class<? extends Mapper>> MAPPER_BY_SORT_KEY = Maps.newHashMap();
    private static final Map<String, Class<? extends WritableComparable>> WRITABLE_BY_SORT_KEY = Maps.newHashMap();

    public static final String NO_SORT = "NO_SORT";

    static {
        MAPPER_BY_SORT_KEY.put(NO_SORT, Mapper.class);
        MAPPER_BY_SORT_KEY.put(GenericJsonStringSortMapper.SORT_KEY, GenericJsonStringSortMapper.class);
        MAPPER_BY_SORT_KEY.put(GenericJsonDateTimeSortMapper.SORT_KEY, GenericJsonDateTimeSortMapper.class);
        MAPPER_BY_SORT_KEY.put(GenericJsonDoubleSortMapper.SORT_KEY, GenericJsonDoubleSortMapper.class);
        MAPPER_BY_SORT_KEY.put(GenericJsonFloatSortMapper.SORT_KEY, GenericJsonFloatSortMapper.class);
        MAPPER_BY_SORT_KEY.put(GenericJsonIntegerSortMapper.SORT_KEY, GenericJsonIntegerSortMapper.class);
        MAPPER_BY_SORT_KEY.put(GenericJsonLongSortMapper.SORT_KEY, GenericJsonLongSortMapper.class);
        MAPPER_BY_SORT_KEY.put(GenericJsonGeohashSortMapper.SORT_KEY, GenericJsonGeohashSortMapper.class);

        WRITABLE_BY_SORT_KEY.put(NO_SORT, LongWritable.class);
        WRITABLE_BY_SORT_KEY.put(GenericJsonStringSortMapper.SORT_KEY, Text.class);
        WRITABLE_BY_SORT_KEY.put(GenericJsonDateTimeSortMapper.SORT_KEY, LongWritable.class);
        WRITABLE_BY_SORT_KEY.put(GenericJsonDoubleSortMapper.SORT_KEY, DoubleWritable.class);
        WRITABLE_BY_SORT_KEY.put(GenericJsonFloatSortMapper.SORT_KEY, FloatWritable.class);
        WRITABLE_BY_SORT_KEY.put(GenericJsonIntegerSortMapper.SORT_KEY, IntWritable.class);
        WRITABLE_BY_SORT_KEY.put(GenericJsonLongSortMapper.SORT_KEY, LongWritable.class);
        WRITABLE_BY_SORT_KEY.put(GenericJsonGeohashSortMapper.SORT_KEY, IntWritable.class);
    }


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
            generateVersion(conf);
            updateHadoopConfiguration(conf, importDefinition.getHadoop());
            return importDefinition;

        } finally {
            IOUtils.closeQuietly(fdis);
        }
    }

    private static String generateVersion(Configuration conf) {
        String version = conf.get(JumboConstants.DELIVERY_VERSION);
        if(version == null) {
            version = UUID.randomUUID().toString();
            conf.set(JumboConstants.DELIVERY_VERSION, version);
        }
        return version;
    }

    public static void setSortConfig(Job conf, List<String> sort) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        conf.getConfiguration().set(JumboConstants.JUMBO_SORT_CONFIG, mapper.writeValueAsString(sort));
    }

    public static void setCollectionInfo(Job conf, String collectionInfo) throws IOException {
        if(collectionInfo == null) {
            return;
        }
        conf.getConfiguration().set(JumboConstants.JUMBO_COLLECTION_INFO, collectionInfo);
    }

    public static void setChecksumType(Job conf, ChecksumType checksumType) throws IOException {
        if(checksumType == null) {
            return;
        }
        conf.getConfiguration().set(JumboConstants.JUMBO_CHECKSUM_TYPE, checksumType.toString());
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
        String importJson = conf.get(JUMBO_INDEX_JSON_CONF);
        if(StringUtils.isBlank(importJson)) {
            throw new IllegalStateException(JUMBO_INDEX_JSON_CONF + " is not set.");
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

    public static List<JumboGenericImportJob> convertToGenericImportJobs(Configuration conf, ImportDefinition importDefinition, String outputWithDate) {
        List<JumboGenericImportJob> result = new LinkedList<JumboGenericImportJob>();
        for (ImportCollection importCollection : importDefinition.getImportCollection()) {
            JumboGenericImportJob job = new JumboGenericImportJob();
            job.setIndexes(importCollection.getIndexes());
            job.setSort(importCollection.getSort());
            job.setCollectionName(importCollection.getCollectionName());
            job.setDeliveryChunkKey(importDefinition.getDeliveryChunkKey());
            job.setDescription(importCollection.getDescription());
            job.setHosts(importDefinition.getHosts());
            job.setChecksumType(importDefinition.getChecksum());
            for (IndexField indexField : job.getIndexes()) {
                if(indexField.getDatePattern() == null) {
                    indexField.setDatePattern(importDefinition.getDatePattern());
                }
            }
            String subPath = importDefinition.getDeliveryChunkKey() + "/" + generateVersion(conf) + "/" + importCollection.getCollectionName() + "/";
            String outputData = outputWithDate + "/data/" + subPath;
            String outputIndex = outputWithDate + "/index/" + subPath;
            String outputLog = outputWithDate + "/log/" + subPath;
            job.setDataStrategy(importCollection.getDataStrategy());
            job.setSortDatePattern(importCollection.getSortDatePattern() != null ? importCollection.getSortDatePattern() : importDefinition.getDatePattern());
            job.setSortType(importCollection.getSortType());
            job.setInputPath(new Path(importCollection.getInput()));
            job.setSortedOutputPath(
              importCollection.getSort() != null && importCollection.getSort().size() > 0 ? new Path(outputData) : null);
            job.setIndexOutputPath(new Path(outputIndex));
            job.setLogOutputPath(new Path(outputLog));
            job.setNumberOfOutputFiles(importCollection.getNumberOfOutputFiles() != null ? importCollection.getNumberOfOutputFiles() : importDefinition.getNumberOfOutputFiles());
            result.add(job);
        }
        return result;
    }

    public static String getOutputPathWithDateStamp(ImportDefinition importDefinition) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        String dateStamp = sdf.format(new Date());
        String output = importDefinition.getOutput();
        return output + "/" + dateStamp;
    }

    public static Set<CommitNotification> convertToImportCommits(ImportDefinition importDefinition) {
        Set<CommitNotification> result = new HashSet<CommitNotification>();
        String deliveryChunkKey = importDefinition.getDeliveryChunkKey();
        List<ImportHost> importHosts = importDefinition.getHosts();
        for (ImportHost importHost : importHosts) {
            CommitNotification commitNotification = new CommitNotification(deliveryChunkKey, importHost, importDefinition.getActivateChunk(), importDefinition.getActivateVersion());
            result.add(commitNotification);
        }
        return result;
    }

    public static Class<? extends Mapper> getSortMapperByType(String type) {
        if(type == null) {
            return MAPPER_BY_SORT_KEY.get(NO_SORT);
        }
        if (!MAPPER_BY_SORT_KEY.containsKey(type)) {
            throw new IllegalArgumentException("Sort type " + type + " is not supported.");
        }
        return MAPPER_BY_SORT_KEY.get(type);
    }

    public static Class<? extends WritableComparable> getSortOutputKeyClassByType(String type) {
        if(type == null) {
            return WRITABLE_BY_SORT_KEY.get(NO_SORT);
        }
        if (!WRITABLE_BY_SORT_KEY.containsKey(type)){
            throw new IllegalArgumentException("Sort type " + type + " is not supported.");
        }
        return WRITABLE_BY_SORT_KEY.get(type);
    }
}
