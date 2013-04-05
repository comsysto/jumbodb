package org.jumbodb.database.service.statistics;

import java.util.Date;

/**
 * User: carsten
 * Date: 3/19/13
 * Time: 2:13 PM
 */
// CARSTEN make spring bean
public class GlobalStatistics {
    private static long numberOfQueries = 0l;
    private static long numberOfResults = 0l;
    private static Date startupTime = new Date();

    public synchronized static void incNumberOfQueries(long numberOfQueries) {
        GlobalStatistics.numberOfQueries += numberOfQueries;
    }

    public synchronized static void incNumberOfResults(long numberOfResults) {
        GlobalStatistics.numberOfResults += numberOfResults;
    }

    public static long getNumberOfQueries() {
        return numberOfQueries;
    }

    public static long getNumberOfResults() {
        return numberOfResults;
    }

    public static Date getStartupTime() {
        return startupTime;
    }
}
