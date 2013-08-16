package org.jumbodb.database.service.statistics;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User: carsten
 * Date: 3/19/13
 * Time: 2:13 PM
 */
// CARSTEN make spring bean
public class GlobalStatistics {
    private static AtomicLong numberOfQueries = new AtomicLong(0l);
    private static AtomicLong numberOfResults = new AtomicLong(0l);
    private static Date startupTime = new Date();

    public static void incNumberOfQueries(long numberOfQueries) {
        GlobalStatistics.numberOfQueries.addAndGet(numberOfQueries);
    }

    public static void incNumberOfResults(long numberOfResults) {
        GlobalStatistics.numberOfResults.addAndGet(numberOfResults);
    }

    public static long getNumberOfQueries() {
        return numberOfQueries.get();
    }

    public static long getNumberOfResults() {
        return numberOfResults.get();
    }

    public static Date getStartupTime() {
        return (Date) startupTime.clone();
    }
}
