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

    public static boolean isDeliveryActive(File activePropsFile) {
        Properties activeProps = PropertiesHelper.loadProperties(activePropsFile);
        return "true".equals(activeProps.getProperty("active"));
    }

    public static boolean isChunkActive(File activePropsFile) {
        Properties activeProps = PropertiesHelper.loadProperties(activePropsFile);
        return Boolean.parseBoolean(activeProps.getProperty("active"));
    }

    public static String getActiveDeliveryVersion(File activePropsFile) {
        Properties activeProps = PropertiesHelper.loadProperties(activePropsFile);
        return activeProps.getProperty("version");
    }

    public static void writeActiveFile(File activeDeliveryFile, String deliveryVersion, boolean active) {
        Properties activeProp = new Properties();
        activeProp.setProperty("active", Boolean.toString(active));
        activeProp.setProperty("version", deliveryVersion);

        FileOutputStream activeDeliveryFos = null;
        try {
            activeDeliveryFos = new FileOutputStream(activeDeliveryFile);
            activeProp.store(activeDeliveryFos, "Active Delivery");
        } catch(IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(activeDeliveryFos);
        }
    }
}
