package org.jumbodb.database.service.configuration;

import java.io.File;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 6:33 PM
 */
public class JumboConfiguration {
    private int queryPort;
    private int importPort;
    private File dataPath;
    private File indexPath;

    public JumboConfiguration(int queryPort, int importPort, File dataPath, File indexPath) {
        this.queryPort = queryPort;
        this.importPort = importPort;
        this.dataPath = dataPath;
        this.indexPath = indexPath;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public int getImportPort() {
        return importPort;
    }

    public File getDataPath() {
        return dataPath;
    }

    public File getIndexPath() {
        return indexPath;
    }

    @Override
    public String toString() {
        return "JumboConfiguration{" +
                "queryPort=" + queryPort +
                ", importPort=" + importPort +
                ", dataPath=" + dataPath +
                ", indexPath=" + indexPath +
                '}';
    }
}
