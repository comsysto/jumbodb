package org.jumbodb.data.common.meta;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
public class CollectionProperties {
    public static final String DEFAULT_FILENAME = "collection.properties";

    public static String getStrategy(File deliveryPropsFile) {
        return getCollectionMeta(deliveryPropsFile).getStrategy();
    }

    public static String getStrategyName(File file) {
        return getCollectionMeta(file).getStrategy();
    }

    public static CollectionMeta getCollectionMeta(File file) {
        Properties properties = PropertiesHelper.loadProperties(file);
        String date = properties.getProperty("date");
        String sourcePath = properties.getProperty("sourcePath");
        String strategy = properties.getProperty("strategy");
        return new CollectionMeta(date, sourcePath, strategy);

    }

    public static void write(File deliveryInfoFile, CollectionMeta collectionMeta) {
        Properties deliveryInfo = new Properties();
        deliveryInfo.setProperty("sourcePath", collectionMeta.getSourcePath());
        deliveryInfo.setProperty("date", collectionMeta.getDate());
        deliveryInfo.setProperty("strategy", collectionMeta.getStrategy());

        FileOutputStream deliveryInfoFos = null;
        try {
            deliveryInfoFos = new FileOutputStream(deliveryInfoFile);
            deliveryInfo.store(deliveryInfoFos, "Collection Information");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(deliveryInfoFos);
        }
    }

    public static class CollectionMeta {
        private String date;
        private String sourcePath;
        private String strategy;

        public CollectionMeta(String date, String sourcePath, String strategy) {
            this.date = date;
            this.sourcePath = sourcePath;
            this.strategy = strategy;
        }


        public String getDate() {
            return date;
        }

        public String getSourcePath() {
            return sourcePath;
        }

        public String getStrategy() {
            return strategy;
        }

    }
}
