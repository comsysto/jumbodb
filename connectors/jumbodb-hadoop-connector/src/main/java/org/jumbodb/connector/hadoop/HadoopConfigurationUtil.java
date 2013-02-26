package org.jumbodb.connector.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
}
