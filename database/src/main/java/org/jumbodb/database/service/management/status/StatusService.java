package org.jumbodb.database.service.management.status;

import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.jumbodb.database.service.importer.DatabaseImportSession;
import org.jumbodb.database.service.importer.ImportTask;
import org.jumbodb.database.service.management.status.dto.ServerInformation;
import org.jumbodb.database.service.query.DatabaseQuerySession;
import org.jumbodb.database.service.query.Restartable;
import org.jumbodb.database.service.statistics.GlobalStatistics;

import java.text.DateFormat;
import java.text.NumberFormat;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:06 PM
 */
public class StatusService {
    private JumboConfiguration config;

    public StatusService(JumboConfiguration config) {
        this.config = config;
    }

    public ServerInformation getStatus() {
        Runtime runtime = Runtime.getRuntime();
        NumberFormat format = NumberFormat.getInstance();
        DateFormat dateFormat = DateFormat.getDateTimeInstance();
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long divideMB = 1024 * 1024;

        ServerInformation status = new ServerInformation();
        status.setAllocatedMemory(format.format(allocatedMemory / divideMB) + " MB");
        status.setDataPath(config.getDataPath().getAbsolutePath());
        status.setFreeMemory(format.format(freeMemory / divideMB) + " MB");
        status.setImportPort(config.getImportPort());
        status.setIndexPath(config.getIndexPath().getAbsolutePath());
        status.setMaximumMemory(format.format(maxMemory / divideMB) + " MB");
        status.setNumberOfQueries(GlobalStatistics.getNumberOfQueries());
        status.setNumberOfResults(GlobalStatistics.getNumberOfResults());
        status.setQueryPort(config.getQueryPort());
        status.setStartupTime(dateFormat.format(GlobalStatistics.getStartupTime()));
        status.setTotalFreeMemory(format.format((freeMemory + (maxMemory - allocatedMemory)) / divideMB) + " MB");
        status.setQueryProtocolVersion(String.valueOf(DatabaseQuerySession.PROTOCOL_VERSION));
        status.setImportProtocolVersion(String.valueOf(DatabaseImportSession.PROTOCOL_VERSION));
        status.setStorageFormatVersion(ImportTask.STORAGE_VERSION);
        return status;
    }
}
