package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class BaseJumboImportJob {
    private String description;
    private String collectionName;
    private String deliveryChunkKey;
    private String dataStrategy;
    private Path inputPath;
    private Path sortedOutputPath;
    private Path indexOutputPath;
    private Path logOutputPath;
    private List<ImportHost> hosts;
    private boolean activateDelivery;

    public Path getSortedOutputPath() {
        return sortedOutputPath;
    }

    public Path getSortedInputPath() {
        if(sortedOutputPath != null) {
            return sortedOutputPath;
        }
        return inputPath;
    }

    public void setSortedOutputPath(Path sortedOutputPath) {
        this.sortedOutputPath = sortedOutputPath;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDataStrategy() {
        return dataStrategy;
    }

    public void setDataStrategy(String dataStrategy) {
        this.dataStrategy = dataStrategy;
    }

    public List<ImportHost> getHosts() {
        return hosts;
    }

    public void setHosts(List<ImportHost> hosts) {
        this.hosts = hosts;
    }

    public boolean isActivateDelivery() {
        return activateDelivery;
    }

    public void setActivateDelivery(boolean activateDelivery) {
        this.activateDelivery = activateDelivery;
    }

    public String getDeliveryChunkKey() {
        return deliveryChunkKey;
    }

    public void setDeliveryChunkKey(String deliveryChunkKey) {
        this.deliveryChunkKey = deliveryChunkKey;
    }

    public Path getInputPath() {
        return inputPath;
    }

    public void setInputPath(Path inputPath) {
        this.inputPath = inputPath;
    }

    public Path getIndexOutputPath() {
        return indexOutputPath;
    }

    public void setIndexOutputPath(Path indexOutputPath) {
        this.indexOutputPath = indexOutputPath;
    }

    public Path getLogOutputPath() {
        return logOutputPath;
    }

    public void setLogOutputPath(Path logOutputPath) {
        this.logOutputPath = logOutputPath;
    }

    @Override
    public String toString() {
        return "JumboGenericImportJob{" +
                ", description='" + description + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", deliveryChunk='" + deliveryChunkKey + '\'' +
                ", dataStrategy='" + dataStrategy + '\'' +
                ", hosts=" + hosts +
                ", activateDelivery=" + activateDelivery +
                '}';
    }
}
