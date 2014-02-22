package org.jumbodb.connector.hadoop.configuration;

import org.jumbodb.connector.hadoop.index.map.GenericJsonStringSortMapper;

import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 3:47 PM
 *
 */
public class ImportCollection {
    private String input;
    private String description;
    private String collectionName;
    private String deliveryChunkKey;
    private String dataStrategy;
    private List<String> sort = new LinkedList<String>();
    private String sortDatePattern;
    private String sortType = GenericJsonStringSortMapper.SORT_KEY;
    private List<IndexField> indexes = new LinkedList<IndexField>();
    private List<ImportHost> hosts;
    private Integer numberOfOutputFiles = null;
    private Boolean activateDelivery = null;


    public ImportCollection() {
    }

    public String getSortDatePattern() {
        return sortDatePattern;
    }

    public void setSortDatePattern(String sortDatePattern) {
        this.sortDatePattern = sortDatePattern;
    }

    public String getSortType() {
        return sortType;
    }

    public void setSortType(String sortType) {
        this.sortType = sortType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean getActivateDelivery() {
        return activateDelivery;
    }

    public void setActivateDelivery(Boolean activateDelivery) {
        this.activateDelivery = activateDelivery;
    }

    public List<ImportHost> getHosts() {
        return hosts;
    }

    public void setHosts(List<ImportHost> hosts) {
        this.hosts = hosts;
    }

    public Integer getNumberOfOutputFiles() {
        return numberOfOutputFiles;
    }

    public void setNumberOfOutputFiles(Integer numberOfOutputFiles) {
        this.numberOfOutputFiles = numberOfOutputFiles;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDeliveryChunkKey() {
        return deliveryChunkKey;
    }

    public void setDeliveryChunkKey(String deliveryChunkKey) {
        this.deliveryChunkKey = deliveryChunkKey;
    }

    public List<String> getSort() {
        return sort;
    }

    public void setSort(List<String> sort) {
        this.sort = sort;
    }

    public List<IndexField> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexField> indexes) {
        this.indexes = indexes;
    }

    public String getDataStrategy() {
        return dataStrategy;
    }

    public void setDataStrategy(String dataStrategy) {
        this.dataStrategy = dataStrategy;
    }


    @Override
    public String toString() {
        return "ImportCollection{" +
                "input='" + input + '\'' +
                ", description='" + description + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", deliveryChunk='" + deliveryChunkKey + '\'' +
                ", dataStrategy='" + dataStrategy + '\'' +
                ", sort=" + sort +
                ", indexes=" + indexes +
                ", hosts=" + hosts +
                ", numberOfOutputFiles=" + numberOfOutputFiles +
                ", activateDelivery=" + activateDelivery +
                '}';
    }
}


