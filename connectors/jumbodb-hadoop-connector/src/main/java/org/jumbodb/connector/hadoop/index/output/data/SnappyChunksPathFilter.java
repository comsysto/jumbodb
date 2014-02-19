package org.jumbodb.connector.hadoop.index.output.data;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author Carsten Hufe
 */
public class SnappyChunksPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        String name = path.getName();
        return !name.endsWith(".chunks");
    }
}