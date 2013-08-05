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


//    public static void writeIndexProperties(ImportMetaIndex information, File deliveryInfoFile) {
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//        Properties deliveryInfo = new Properties();
//        deliveryInfo.setProperty("deliveryVersion", information.getDeliveryVersion());
//        deliveryInfo.setProperty("date", sdf.format(new Date()));
//        deliveryInfo.setProperty("indexName", information.getIndexName());
//        deliveryInfo.setProperty("strategy", information.getStrategy());
//        deliveryInfo.setProperty("indexSourceFields", information.getIndexSourceFields());
//
//        FileOutputStream deliveryInfoFos = null;
//        try {
//            deliveryInfoFos = new FileOutputStream(deliveryInfoFile);
//            deliveryInfo.store(deliveryInfoFos, "Delivery Information");
//        } catch(IOException e) {
//            throw new RuntimeException(e);
//        } finally {
//            IOUtils.closeQuietly(deliveryInfoFos);
//        }
//    }
//
//    public static void writeDataDeliveryProperties(ImportMetaData information, File deliveryInfoFile) {
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
//        Properties deliveryInfo = new Properties();
//        deliveryInfo.setProperty("deliveryVersion", information.getDeliveryVersion());
//        deliveryInfo.setProperty("sourcePath", information.getSourcePath());
//        deliveryInfo.setProperty("date", sdf.format(new Date()));
//        deliveryInfo.setProperty("info", information.getInfo());
//        deliveryInfo.setProperty("strategy", information.getDataStrategy());
//        FileOutputStream deliveryInfoFos = null;
//        try {
//            deliveryInfoFos = new FileOutputStream(deliveryInfoFile);
//            deliveryInfo.store(deliveryInfoFos, "Delivery Information");
//        } catch(IOException e) {
//            throw new RuntimeException(e);
//        } finally {
//            IOUtils.closeQuietly(deliveryInfoFos);
//
//        }
//    }
}
