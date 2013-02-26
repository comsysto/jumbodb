package org.jumbodb.connector.importer;

import java.io.OutputStream;

/**
 * User: carsten
 * Date: 2/26/13
 * Time: 4:50 PM
 */
public interface OnCopyCallback {
    void onCopy(OutputStream outputStream);
}
