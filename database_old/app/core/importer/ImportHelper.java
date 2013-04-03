package core.importer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.IOUtils;

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
        Config activeProps = ConfigFactory.parseFile(activeDeliveryFile);
        return activeProps.getString("deliveryVersion");
    }
}
