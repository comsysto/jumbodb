package org.jumbodb.database.service.query.definition;

import java.io.File;

/**
 * @author Carsten Hufe
 */
public class IndexDefinition implements Comparable<IndexDefinition> {
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
    public int compareTo(IndexDefinition o) {
        return name.compareTo(o.name);
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
