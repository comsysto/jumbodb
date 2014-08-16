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
    private Boolean activateChunk = JumboConstants.DELIVERY_ACTIVATE_CHUNK;
    private Boolean activateVersion = JumboConstants.DELIVERY_ACTIVATE_VERSION;
    private List<ImportCollection> importCollection;
    private List<HadoopConfig> hadoop = new LinkedList<HadoopConfig>();
    private String output;
    private Integer numberOfOutputFiles = 50;
    private String datePattern = "yyyy-MM-dd HH:mm:ss";

    public String getDatePattern() {
        return datePattern;
    }

    public void setDatePattern(String datePattern) {
        this.datePattern = datePattern;
    }

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

    public Boolean getActivateChunk() {
        return activateChunk;
    }

    public void setActivateChunk(Boolean activateChunk) {
        this.activateChunk = activateChunk;
    }

    public Boolean getActivateVersion() {
        return activateVersion;
    }

    public void setActivateVersion(Boolean activateVersion) {
        this.activateVersion = activateVersion;
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
