package core.query;

import java.io.IOException;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 4:16 PM
 */
public interface ResultCallback {
    void writeResult(byte[] result) throws IOException;
}
