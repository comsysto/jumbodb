package org.jumbodb.database.service.exporter;

import org.jumbodb.connector.importer.MonitorCountingOutputStream;

import java.io.OutputStream;

/**
 * @author Carsten Hufe
 */
public class ExportDeliveryCountOutputStream extends MonitorCountingOutputStream {
    private ExportDelivery exportDelivery;

    /**
     * Constructs a new CountingOutputStream.
     *
     * @param out the OutputStream to write to
     */
    public ExportDeliveryCountOutputStream(OutputStream out, ExportDelivery exportDelivery) {
        super(out, 5000);
        this.exportDelivery = exportDelivery;
    }

    @Override
    protected void onInterval(long speedInBytesPerSecond) {
        super.onInterval(speedInBytesPerSecond);
        exportDelivery.setCopyRateInBytesUncompressed(speedInBytesPerSecond);
    }
}
