package org.jumbodb.database.rest.controller;

import org.jumbodb.database.rest.dto.Helloworld;
import org.jumbodb.database.rest.dto.status.ServerInformation;
import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.jumbodb.database.service.statistics.GlobalStatistics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.DateFormat;
import java.text.NumberFormat;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 11:45 AM
 */
@Controller
public class StatusController {
    @Autowired
    private JumboConfiguration jumboConfiguration;

    @RequestMapping(value = "/rest/status", method = RequestMethod.GET)
    @ResponseBody
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
        status.setDataPath(jumboConfiguration.getDataPath().getAbsolutePath());
        status.setFreeMemory(format.format(freeMemory / divideMB) + " MB");
        status.setImportPort(jumboConfiguration.getImportPort());
        status.setIndexPath(jumboConfiguration.getIndexPath().getAbsolutePath());
        status.setMaximumMemory(format.format(maxMemory / divideMB) + " MB");
        status.setNumberOfQueries(GlobalStatistics.getNumberOfQueries());
        status.setNumberOfResults(GlobalStatistics.getNumberOfResults());
        status.setQueryPort(jumboConfiguration.getQueryPort());
        status.setStartupTime(dateFormat.format(GlobalStatistics.getStartupTime()));
        status.setTotalFreeMemory(format.format((freeMemory + (maxMemory - allocatedMemory)) / divideMB) + " MB");
        return status;
    }
}
