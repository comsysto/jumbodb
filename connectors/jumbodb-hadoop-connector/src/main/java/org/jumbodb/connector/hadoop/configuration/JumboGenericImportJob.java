package org.jumbodb.connector.hadoop.configuration;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class JumboGenericImportJob extends BaseJumboImportJob {
    private List<String> sort = new LinkedList<String>();
    private String sortType;
    private String sortDatePattern;
    private List<IndexField> indexes = new LinkedList<IndexField>();

    public String getSortType() {
        return sortType;
    }

    public void setSortType(String sortType) {
        this.sortType = sortType;
    }

    public String getSortDatePattern() {
        return sortDatePattern;
    }

    public void setSortDatePattern(String sortDatePattern) {
        this.sortDatePattern = sortDatePattern;
    }

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
