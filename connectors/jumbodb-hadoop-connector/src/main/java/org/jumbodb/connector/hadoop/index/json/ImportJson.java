package org.jumbodb.connector.hadoop.index.json;

import org.jumbodb.connector.hadoop.JumboConstants;

import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 3:47 PM
 *
 */
public class ImportJson {
    private String input;
    private String output;
    private String collectionName;
    private String deliveryChunk;
    private List<String> sort = new LinkedList<String>();
    private List<IndexJson> indexes = new LinkedList<IndexJson>();
    private List<HostsJson> hosts;
    private int numberOfOutputFiles = 50;
    private boolean activateDelivery = JumboConstants.DELIVERY_ACTIVATE_DEFAULT;
    private List<HadoopJsonConfig> hadoop = new LinkedList<HadoopJsonConfig>();

    public ImportJson() {
    }

    public boolean isActivateDelivery() {
        return activateDelivery;
    }

    public void setActivateDelivery(boolean activateDelivery) {
        this.activateDelivery = activateDelivery;
    }

    public List<HostsJson> getHosts() {
        return hosts;
    }

    public void setHosts(List<HostsJson> hosts) {
        this.hosts = hosts;
    }

    public List<HadoopJsonConfig> getHadoop() {
        return hadoop;
    }

    public void setHadoop(List<HadoopJsonConfig> hadoop) {
        this.hadoop = hadoop;
    }

    public int getNumberOfOutputFiles() {
        return numberOfOutputFiles;
    }

    public void setNumberOfOutputFiles(int numberOfOutputFiles) {
        this.numberOfOutputFiles = numberOfOutputFiles;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getDeliveryChunk() {
        return deliveryChunk;
    }

    public void setDeliveryChunk(String deliveryChunk) {
        this.deliveryChunk = deliveryChunk;
    }

    public List<String> getSort() {
        return sort;
    }

    public void setSort(List<String> sort) {
        this.sort = sort;
    }

    public List<IndexJson> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexJson> indexes) {
        this.indexes = indexes;
    }



    @Override
    public String toString() {
        return "ImportJson{" +
                "input='" + input + '\'' +
                ", output='" + output + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", deliveryChunk='" + deliveryChunk + '\'' +
                ", sort=" + sort +
                ", indexes=" + indexes +
                ", hosts=" + hosts +
                ", numberOfOutputFiles=" + numberOfOutputFiles +
                ", hadoop=" + hadoop +
                '}';
    }
}


