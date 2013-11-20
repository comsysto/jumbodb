package org.jumbodb.data.common.meta;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
public class IndexProperties {
    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm";
    public static final String DEFAULT_FILENAME = "index.properties";

    public static String getStrategy(File indexPropsFile) {
        Properties activeProps = PropertiesHelper.loadProperties(indexPropsFile);
        return activeProps.getProperty("strategy");
    }

    public static IndexMeta getIndexMeta(File file) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
        Properties properties = PropertiesHelper.loadProperties(file);
        String deliveryVersion = properties.getProperty("deliveryVersion");
        String date = properties.getProperty("date");
        String indexName = properties.getProperty("indexName");
        String strategy = properties.getProperty("strategy");
        String indexSourceFields = properties.getProperty("indexSourceFields");
        try {
            return  new IndexMeta(deliveryVersion, sdf.parse(date), indexName, strategy, indexSourceFields);
        } catch (ParseException e) {
            throw new UnhandledException(e);
        }
    }

    public static void write(File indexFile, IndexMeta indexMeta) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
        Properties indexInfo = new Properties();
        indexInfo.setProperty("deliveryVersion", indexMeta.getDeliveryVersion());
        indexInfo.setProperty("date", sdf.format(indexMeta.getDate()));
        indexInfo.setProperty("indexName", indexMeta.getIndexName());
        indexInfo.setProperty("strategy", indexMeta.getStrategy());
        indexInfo.setProperty("indexSourceFields", indexMeta.getIndexSourceFields());

        FileOutputStream deliveryInfoFos = null;
        try {
            deliveryInfoFos = new FileOutputStream(indexFile);
            indexInfo.store(deliveryInfoFos, "Index Information");
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(deliveryInfoFos);
        }
    }

    public static class IndexMeta {
        private String deliveryVersion;
        private Date date;
        private String indexName;
        private String strategy;
        private String indexSourceFields;

        public IndexMeta(String deliveryVersion, Date date, String indexName, String strategy, String indexSourceFields) {
            this.deliveryVersion = deliveryVersion;
            this.date = date;
            this.indexName = indexName;
            this.strategy = strategy;
            this.indexSourceFields = indexSourceFields;
        }

        public String getDeliveryVersion() {
            return deliveryVersion;
        }

        public Date getDate() {
            return date;
        }

        public String getIndexName() {
            return indexName;
        }

        public String getStrategy() {
            return strategy;
        }

        public String getIndexSourceFields() {
            return indexSourceFields;
        }
    }
}
