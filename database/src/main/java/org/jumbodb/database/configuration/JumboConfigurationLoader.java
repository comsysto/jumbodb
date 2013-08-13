package org.jumbodb.database.configuration;

import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * User: carsten
 * Date: 4/5/13
 * Time: 10:30 AM
 */
public class JumboConfigurationLoader {
    private File globalConfigFile;

    public JumboConfigurationLoader(File globalConfigFile) {
        this.globalConfigFile = globalConfigFile;
    }

    public Properties loadConfiguration() throws IOException {
        Properties properties = new Properties();
        PropertiesLoaderUtils.fillProperties(properties, new ClassPathResource("org/jumbodb/database/service/jumbodb.conf"));
        if(globalConfigFile.exists()) {
            PropertiesLoaderUtils.fillProperties(properties, new FileSystemResource(globalConfigFile));
        }
        String jumboConfig = System.getProperty("jumbodb.config");
        if (StringUtils.isNotEmpty(jumboConfig)) {
            PropertiesLoaderUtils.fillProperties(properties, new FileSystemResource(jumboConfig));
        }
        String userHome = System.getProperty("user.home");
        Set<String> keys = properties.stringPropertyNames();
        Properties result = new Properties();
        for (String key : keys) {
            String property = properties.getProperty(key);
            property = StringUtils.replace(property, "$USER_HOME", userHome);
            property = StringUtils.replace(property, "%USER_HOME%", userHome);
            result.setProperty(key, property);
        }
        return result;
    }
}
