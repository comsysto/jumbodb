package org.jumbodb.database.service.management.storage.dto.maintenance;

/**
 * @author Carsten Hufe
 */
public class TemporaryFiles {
    private String temporaryDataSize;
    private int numberOfAbortedDeliveries;
    private boolean importRunning;

    public TemporaryFiles(String temporaryDataSize, int numberOfAbortedDeliveries, boolean importRunning) {
        this.temporaryDataSize = temporaryDataSize;
        this.numberOfAbortedDeliveries = numberOfAbortedDeliveries;
        this.importRunning = importRunning;
    }

    public String getTemporaryDataSize() {
        return temporaryDataSize;
    }

    public int getNumberOfAbortedDeliveries() {
        return numberOfAbortedDeliveries;
    }

    public boolean isImportRunning() {
        return importRunning;
    }

    @Override
    public String toString() {
        return "TemporaryFiles{" +
                "temporaryDataSize='" + temporaryDataSize + '\'' +
                ", numberOfAbortedDeliveries=" + numberOfAbortedDeliveries +
                ", importRunning=" + importRunning +
                '}';
    }
}
