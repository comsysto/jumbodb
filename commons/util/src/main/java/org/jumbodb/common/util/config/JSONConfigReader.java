package org.jumbodb.common.util.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author Ulf Gitschthaler
 */
public class JSONConfigReader {

    private static final String USER_HOME_PLACEHOLDER_1 = "$USER_HOME";
    private static final String USER_HOME_PLACEHOLDER_2 = "%USER_HOME%";
    private static final String USER_HOME_PATH = System.getProperty("user.home");


    public static <T> T read(Class<T> destinationClazz, String jsonConfigPath) throws IOException {
        String configContent = FileUtils.readFileToString(new File(jsonConfigPath), "UTF-8");
        configContent = replaceUserHome(configContent);
        return new ObjectMapper().readValue(configContent, destinationClazz);
    }

    protected static String replaceUserHome(String configContent) {
        String destination = StringUtils.replace(configContent, USER_HOME_PLACEHOLDER_1, USER_HOME_PATH);
        return StringUtils.replace(destination, USER_HOME_PLACEHOLDER_2, USER_HOME_PATH);
    }
}
