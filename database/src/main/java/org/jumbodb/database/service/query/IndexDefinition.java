package org.jumbodb.database.service.query;

import java.io.File;

/**
 * @author Carsten Hufe
 */
public class IndexDefinition {
    private String name;
    private File path;
    private String strategy;

    public IndexDefinition(String name, File path, String strategy) {
        this.name = name;
        this.path = path;
        this.strategy = strategy;
    }

    public String getName() {
        return name;
    }

    public File getPath() {
        return path;
    }

    public String getStrategy() {
        return strategy;
    }

    @Override
    public String toString() {
        return "IndexDefinition{" +
                "name='" + name + '\'' +
                ", path=" + path +
                ", strategy='" + strategy + '\'' +
                '}';
    }
}
