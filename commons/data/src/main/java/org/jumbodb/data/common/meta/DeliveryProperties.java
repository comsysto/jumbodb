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
public class DeliveryProperties {
    public static final String DEFAULT_FILENAME = "delivery.properties";
    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm";

    public static String getStrategy(File deliveryPropsFile) {
        return getDeliveryMeta(deliveryPropsFile).getStrategy();
    }

    public static Date getDate(File file) {
        return getDeliveryMeta(file).getDate();
    }

    public static DeliveryMeta getDeliveryMeta(File file) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
        Properties properties = PropertiesHelper.loadProperties(file);
        String deliveryVersion = properties.getProperty("deliveryVersion"); // CARSTEN remove
        String info = properties.getProperty("info"); // CARSTEN remove
        String date = properties.getProperty("date");
        String sourcePath = properties.getProperty("sourcePath");
        String strategy = properties.getProperty("strategy");
        try {
            return  new DeliveryMeta(deliveryVersion, info, sdf.parse(date), sourcePath, strategy);
        } catch (ParseException e) {
            throw new UnhandledException(e);
        }

    }

    // CARSTEN remove
    public static void write(File deliveryInfoFile, DeliveryMeta deliveryMeta) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
        Properties deliveryInfo = new Properties();
        deliveryInfo.setProperty("deliveryVersion", deliveryMeta.getDeliveryVersion());
        deliveryInfo.setProperty("sourcePath", deliveryMeta.getSourcePath());
        deliveryInfo.setProperty("date", sdf.format(deliveryMeta.getDate()));
        deliveryInfo.setProperty("info", deliveryMeta.getInfo());
        deliveryInfo.setProperty("strategy", deliveryMeta.getStrategy());

        FileOutputStream deliveryInfoFos = null;
        try {
            deliveryInfoFos = new FileOutputStream(deliveryInfoFile);
            deliveryInfo.store(deliveryInfoFos, "Delivery Information");
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(deliveryInfoFos);
        }
    }

    public static class DeliveryMeta {
        private String deliveryVersion;
        private String info;
        private Date date;
        private String sourcePath;
        private String strategy;

        public DeliveryMeta(String deliveryVersion, String info, Date date, String sourcePath, String strategy) {
            this.deliveryVersion = deliveryVersion;
            this.info = info;
            this.date = date;
            this.sourcePath = sourcePath;
            this.strategy = strategy;
        }

        public String getInfo() {
            return info;
        }

        public Date getDate() {
            return date;
        }

        public String getSourcePath() {
            return sourcePath;
        }

        public String getStrategy() {
            return strategy;
        }

        public String getDeliveryVersion() {
            return deliveryVersion;
        }
    }
}
