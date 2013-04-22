package org.jumbodb.connector.hadoop.configuration;

import org.jumbodb.connector.hadoop.JumboConstants;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class ImportDefinition {
    private String deliveryChunkKey;
    private List<ImportHost> hosts;
    private String description = "No description";
    private Boolean activateDelivery = JumboConstants.DELIVERY_ACTIVATE_DEFAULT;
    private List<ImportCollection> importCollection;
    private List<HadoopConfig> hadoop = new LinkedList<HadoopConfig>();
    private String output;
    private Integer numberOfOutputFiles = 50;

    public Integer getNumberOfOutputFiles() {
        return numberOfOutputFiles;
    }

    public void setNumberOfOutputFiles(Integer numberOfOutputFiles) {
        this.numberOfOutputFiles = numberOfOutputFiles;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getDeliveryChunkKey() {
        return deliveryChunkKey;
    }

    public void setDeliveryChunkKey(String deliveryChunkKey) {
        this.deliveryChunkKey = deliveryChunkKey;
    }

    public List<ImportHost> getHosts() {
        return hosts;
    }

    public void setHosts(List<ImportHost> hosts) {
        this.hosts = hosts;
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

    public List<ImportCollection> getImportCollection() {
        return importCollection;
    }

    public void setImportCollection(List<ImportCollection> importCollection) {
        this.importCollection = importCollection;
    }

    public List<HadoopConfig> getHadoop() {
        return hadoop;
    }

    public void setHadoop(List<HadoopConfig> hadoop) {
        this.hadoop = hadoop;
    }
}
