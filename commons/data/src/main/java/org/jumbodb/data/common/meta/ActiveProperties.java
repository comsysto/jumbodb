package org.jumbodb.data.common.meta;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
public class ActiveProperties {
    public static final String DEFAULT_FILENAME = "active.properties";

    public static String getActiveDeliveryVersion(File activePropsFile) {
        Properties activeProps = PropertiesHelper.loadProperties(activePropsFile);
        return activeProps.getProperty("deliveryVersion");
    }

    public static void writeActiveFile(File activeDeliveryFile, String deliveryVersion) {
        Properties active = new Properties();
        active.setProperty("active", "true");
        active.setProperty("deliveryVersion", deliveryVersion);

        FileOutputStream activeDeliveryFos = null;
        try {
            activeDeliveryFos = new FileOutputStream(activeDeliveryFile);
            active.store(activeDeliveryFos, "Active Delivery");
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(activeDeliveryFos);
        }
    }
}
