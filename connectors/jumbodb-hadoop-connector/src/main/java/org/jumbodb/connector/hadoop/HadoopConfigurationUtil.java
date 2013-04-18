package org.jumbodb.connector.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.index.json.HadoopJsonConfig;
import org.jumbodb.connector.hadoop.index.json.ImportJson;
import org.jumbodb.connector.hadoop.index.json.IndexJson;
import org.jumbodb.connector.hadoop.index.map.GenericJsonIndexMapper;

import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.Properties;

/**
 * User: carsten
 * Date: 11/2/12
 * Time: 1:22 PM
 */
public class HadoopConfigurationUtil {

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


    public static ImportJson loadJsonConfigurationAndUpdateHadoop(String configFileLocation, Configuration conf) throws IOException {
        FSDataInputStream fdis = null;
        try {
            Path configFilePath = new Path(configFileLocation);
            FileSystem fs = FileSystem.get(URI.create(configFileLocation), conf);
            fdis = fs.open(configFilePath);
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
            ImportJson importJson = mapper.readValue(fdis, ImportJson.class);
            conf.set(JumboConstants.JUMBO_JSON_CONF, mapper.writeValueAsString(importJson));
            updateHadoopConfiguration(conf, importJson);
            return importJson;

        } finally {
            IOUtils.closeQuietly(fdis);
        }
    }

    public static ImportJson loadImportJson(Configuration conf) throws IOException {
        String importJson = conf.get(JumboConstants.JUMBO_JSON_CONF);
        if(StringUtils.isBlank(importJson)) {
            throw new IllegalStateException(JumboConstants.JUMBO_JSON_CONF + " is not set.");
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        return mapper.readValue(importJson, ImportJson.class);
    }

    public static IndexJson loadIndexJson(Configuration conf) throws IOException {
        String importJson = conf.get(GenericJsonIndexMapper.JUMBO_INDEX_JSON_CONF);
        if(StringUtils.isBlank(importJson)) {
            throw new IllegalStateException(GenericJsonIndexMapper.JUMBO_INDEX_JSON_CONF + " is not set.");
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        return mapper.readValue(importJson, IndexJson.class);
    }

    public static void updateHadoopConfiguration(Configuration conf, ImportJson importJson) {
        for (HadoopJsonConfig config : importJson.getHadoop()) {
            conf.set(config.getKey(), config.getValue());
        }
    }
}
