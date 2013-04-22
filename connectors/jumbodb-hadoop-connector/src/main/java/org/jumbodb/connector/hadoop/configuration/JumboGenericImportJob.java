package org.jumbodb.connector.hadoop.configuration;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class JumboGenericImportJob extends BaseJumboImportJob {
    private List<String> sort = new LinkedList<String>();
    private List<IndexField> indexes = new LinkedList<IndexField>();

    public List<IndexField> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexField> indexes) {
        this.indexes = indexes;
    }

    public List<String> getSort() {
        return sort;
    }

    public void setSort(List<String> sort) {
        this.sort = sort;
    }

    @Override
    public String toString() {
        return "JumboGenericImportJob{" +
                "sort=" + sort +
                ", indexes=" + indexes +
                '}';
    }
}
