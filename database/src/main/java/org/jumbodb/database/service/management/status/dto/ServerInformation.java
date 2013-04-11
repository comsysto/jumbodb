package org.jumbodb.database.service.management.status.dto;

import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 6:27 PM
 */

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ServerInformation {
    private Integer queryPort;
    private Integer importPort;
    private String dataPath;
    private String indexPath;
    private String maximumMemory;
    private String allocatedMemory;
    private String freeMemory;
    private String totalFreeMemory;
    private Long numberOfQueries;
    private Long numberOfResults;
    private String startupTime;
    private String queryProtocolVersion;
    private String importProtocolVersion;
    private String storageFormatVersion;
    private String indexDiskUsedSpace;
    private String indexDiskTotalSpace;
    private String indexDiskFreeSpace;
    private Double indexDiskUsedSpacePerc;
    private String dataDiskUsedSpace;
    private String dataDiskTotalSpace;
    private String dataDiskFreeSpace;
    private Double dataDiskUsedSpacePerc;

    public String getIndexDiskTotalSpace() {
        return indexDiskTotalSpace;
    }

    public void setIndexDiskTotalSpace(String indexDiskTotalSpace) {
        this.indexDiskTotalSpace = indexDiskTotalSpace;
    }

    public String getIndexDiskFreeSpace() {
        return indexDiskFreeSpace;
    }

    public void setIndexDiskFreeSpace(String indexDiskFreeSpace) {
        this.indexDiskFreeSpace = indexDiskFreeSpace;
    }

    public Double getIndexDiskUsedSpacePerc() {
        return indexDiskUsedSpacePerc;
    }

    public void setIndexDiskUsedSpacePerc(Double indexDiskUsedSpacePerc) {
        this.indexDiskUsedSpacePerc = indexDiskUsedSpacePerc;
    }

    public String getDataDiskTotalSpace() {
        return dataDiskTotalSpace;
    }

    public void setDataDiskTotalSpace(String dataDiskTotalSpace) {
        this.dataDiskTotalSpace = dataDiskTotalSpace;
    }

    public String getDataDiskFreeSpace() {
        return dataDiskFreeSpace;
    }

    public void setDataDiskFreeSpace(String dataDiskFreeSpace) {
        this.dataDiskFreeSpace = dataDiskFreeSpace;
    }

    public Double getDataDiskUsedSpacePerc() {
        return dataDiskUsedSpacePerc;
    }

    public void setDataDiskUsedSpacePerc(Double dataDiskUsedSpacePerc) {
        this.dataDiskUsedSpacePerc = dataDiskUsedSpacePerc;
    }

    public Integer getQueryPort() {
        return queryPort;
    }

    public void setQueryPort(Integer queryPort) {
        this.queryPort = queryPort;
    }

    public Integer getImportPort() {
        return importPort;
    }

    public void setImportPort(Integer importPort) {
        this.importPort = importPort;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }

    public String getMaximumMemory() {
        return maximumMemory;
    }

    public void setMaximumMemory(String maximumMemory) {
        this.maximumMemory = maximumMemory;
    }

    public String getAllocatedMemory() {
        return allocatedMemory;
    }

    public void setAllocatedMemory(String allocatedMemory) {
        this.allocatedMemory = allocatedMemory;
    }

    public String getFreeMemory() {
        return freeMemory;
    }

    public void setFreeMemory(String freeMemory) {
        this.freeMemory = freeMemory;
    }

    public String getTotalFreeMemory() {
        return totalFreeMemory;
    }

    public void setTotalFreeMemory(String totalFreeMemory) {
        this.totalFreeMemory = totalFreeMemory;
    }

    public Long getNumberOfQueries() {
        return numberOfQueries;
    }

    public void setNumberOfQueries(Long numberOfQueries) {
        this.numberOfQueries = numberOfQueries;
    }

    public Long getNumberOfResults() {
        return numberOfResults;
    }

    public void setNumberOfResults(Long numberOfResults) {
        this.numberOfResults = numberOfResults;
    }

    public String getStartupTime() {
        return startupTime;
    }

    public void setStartupTime(String startupTime) {
        this.startupTime = startupTime;
    }

    public String getQueryProtocolVersion() {
        return queryProtocolVersion;
    }

    public void setQueryProtocolVersion(String queryProtocolVersion) {
        this.queryProtocolVersion = queryProtocolVersion;
    }

    public String getImportProtocolVersion() {
        return importProtocolVersion;
    }

    public void setImportProtocolVersion(String importProtocolVersion) {
        this.importProtocolVersion = importProtocolVersion;
    }

    public String getStorageFormatVersion() {
        return storageFormatVersion;
    }

    public void setStorageFormatVersion(String storageFormatVersion) {
        this.storageFormatVersion = storageFormatVersion;
    }

    public String getIndexDiskUsedSpace() {
        return indexDiskUsedSpace;
    }

    public void setIndexDiskUsedSpace(String indexDiskUsedSpace) {
        this.indexDiskUsedSpace = indexDiskUsedSpace;
    }

    public String getDataDiskUsedSpace() {
        return dataDiskUsedSpace;
    }

    public void setDataDiskUsedSpace(String dataDiskUsedSpace) {
        this.dataDiskUsedSpace = dataDiskUsedSpace;
    }

    @Override
    public String toString() {
        return "ServerInformation{" +
                "queryPort=" + queryPort +
                ", importPort=" + importPort +
                ", dataPath='" + dataPath + '\'' +
                ", indexPath='" + indexPath + '\'' +
                ", maximumMemory='" + maximumMemory + '\'' +
                ", allocatedMemory='" + allocatedMemory + '\'' +
                ", freeMemory='" + freeMemory + '\'' +
                ", totalFreeMemory='" + totalFreeMemory + '\'' +
                ", numberOfQueries=" + numberOfQueries +
                ", numberOfResults=" + numberOfResults +
                ", startupTime='" + startupTime + '\'' +
                ", queryProtocolVersion='" + queryProtocolVersion + '\'' +
                ", importProtocolVersion='" + importProtocolVersion + '\'' +
                ", storageFormatVersion='" + storageFormatVersion + '\'' +
                ", indexDiskUsedSpace='" + indexDiskUsedSpace + '\'' +
                ", indexDiskTotalSpace='" + indexDiskTotalSpace + '\'' +
                ", indexDiskFreeSpace='" + indexDiskFreeSpace + '\'' +
                ", indexDiskUsedSpacePerc=" + indexDiskUsedSpacePerc +
                ", dataDiskUsedSpace='" + dataDiskUsedSpace + '\'' +
                ", dataDiskTotalSpace='" + dataDiskTotalSpace + '\'' +
                ", dataDiskFreeSpace='" + dataDiskFreeSpace + '\'' +
                ", dataDiskUsedSpacePerc=" + dataDiskUsedSpacePerc +
                '}';
    }
}
