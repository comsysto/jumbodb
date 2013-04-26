package org.jumbodb.database.service.importer;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * User: carsten
 * Date: 3/22/13
 * Time: 4:02 PM
 */
public class ImportHelper {

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

    public static String getActiveDeliveryVersion(File activeDeliveryFile) {
        try {
            Properties activeProps = PropertiesLoaderUtils.loadProperties(new FileSystemResource(activeDeliveryFile));
            return activeProps.getProperty("deliveryVersion");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
