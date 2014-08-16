package org.jumbodb.data.common.meta;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
public class DeliveryProperties {
    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_FILENAME = "delivery.properties";

    public static DeliveryMeta getDeliveryMeta(File file) {
        Properties properties = PropertiesHelper.loadProperties(file);
        String date = properties.getProperty("date");
        String info = properties.getProperty("info");
        String delivery = properties.getProperty("delivery");
        String version = properties.getProperty("version");
        return new DeliveryMeta(delivery, version, date, info);
    }

    public static void write(File deliveryFile, DeliveryMeta deliveryMeta) {
        Properties active = new Properties();
        active.setProperty("delivery", deliveryMeta.getDelivery());
        active.setProperty("version", deliveryMeta.getVersion());
        active.setProperty("date", deliveryMeta.getDate());
        active.setProperty("info", deliveryMeta.getInfo());

        FileOutputStream activeDeliveryFos = null;
        try {
            activeDeliveryFos = new FileOutputStream(deliveryFile);
            active.store(activeDeliveryFos, "Delivery information");
        } catch (IOException e) {
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

        public DeliveryMeta(String delivery, String version, String date, String info) {
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
