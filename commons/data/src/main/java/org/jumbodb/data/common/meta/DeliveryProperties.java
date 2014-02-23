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
// CARSTEN unit test
public class DeliveryProperties {
    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_FILENAME = "delivery.properties";

    public static DeliveryMeta getDeliveryMeta(File file) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
        Properties properties = PropertiesHelper.loadProperties(file);
        String date = properties.getProperty("date");
        String info = properties.getProperty("info");
        String delivery = properties.getProperty("delivery");
        String version = properties.getProperty("version");
        return  new DeliveryMeta(date, info, delivery, version);
    }

    public static void writeDeliveryFile(File deliveryFile, String deliveryKey, String deliveryVersion, String info) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);

        Properties active = new Properties();
        active.setProperty("version", deliveryVersion);
        active.setProperty("date", sdf.format(new Date()));
        active.setProperty("info", info);
        active.setProperty("delivery", deliveryKey);

        FileOutputStream activeDeliveryFos = null;
        try {
            activeDeliveryFos = new FileOutputStream(deliveryFile);
            active.store(activeDeliveryFos, "Delivery information");
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(activeDeliveryFos);
        }
    }

    public static class DeliveryMeta {
        private String date;
        private String info;
        private String delivery;
        private String version;

        public DeliveryMeta(String date, String info, String delivery, String version) {
            this.date = date;
            this.info = info;
            this.delivery = delivery;
            this.version = version;
        }

        public String getDate() {
            return date;
        }

        public String getInfo() {
            return info;
        }

        public String getDelivery() {
            return delivery;
        }

        public String getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "DeliveryMeta{" +
                    "date=" + date +
                    ", info='" + info + '\'' +
                    ", delivery='" + delivery + '\'' +
                    ", version='" + version + '\'' +
                    '}';
        }
    }
}
