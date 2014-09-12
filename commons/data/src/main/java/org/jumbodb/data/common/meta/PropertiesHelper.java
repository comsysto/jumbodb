package org.jumbodb.data.common.meta;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
public class PropertiesHelper {
    public static Properties loadProperties(File file) {
        Properties props = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            props.load(fis);
        } catch (FileNotFoundException e) {
            throw new UnhandledException(e);
        } catch (IOException e) {
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(fis);
        }
        return props;

    }
}
