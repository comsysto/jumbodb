package org.jumbodb.database.rest.dto.status;

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
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ServerInformation that = (ServerInformation) o;

        if (allocatedMemory != null ? !allocatedMemory.equals(that.allocatedMemory) : that.allocatedMemory != null)
            return false;
        if (dataPath != null ? !dataPath.equals(that.dataPath) : that.dataPath != null) return false;
        if (freeMemory != null ? !freeMemory.equals(that.freeMemory) : that.freeMemory != null) return false;
        if (importPort != null ? !importPort.equals(that.importPort) : that.importPort != null) return false;
        if (indexPath != null ? !indexPath.equals(that.indexPath) : that.indexPath != null) return false;
        if (maximumMemory != null ? !maximumMemory.equals(that.maximumMemory) : that.maximumMemory != null)
            return false;
        if (numberOfQueries != null ? !numberOfQueries.equals(that.numberOfQueries) : that.numberOfQueries != null)
            return false;
        if (numberOfResults != null ? !numberOfResults.equals(that.numberOfResults) : that.numberOfResults != null)
            return false;
        if (queryPort != null ? !queryPort.equals(that.queryPort) : that.queryPort != null) return false;
        if (startupTime != null ? !startupTime.equals(that.startupTime) : that.startupTime != null) return false;
        if (totalFreeMemory != null ? !totalFreeMemory.equals(that.totalFreeMemory) : that.totalFreeMemory != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = queryPort != null ? queryPort.hashCode() : 0;
        result = 31 * result + (importPort != null ? importPort.hashCode() : 0);
        result = 31 * result + (dataPath != null ? dataPath.hashCode() : 0);
        result = 31 * result + (indexPath != null ? indexPath.hashCode() : 0);
        result = 31 * result + (maximumMemory != null ? maximumMemory.hashCode() : 0);
        result = 31 * result + (allocatedMemory != null ? allocatedMemory.hashCode() : 0);
        result = 31 * result + (freeMemory != null ? freeMemory.hashCode() : 0);
        result = 31 * result + (totalFreeMemory != null ? totalFreeMemory.hashCode() : 0);
        result = 31 * result + (numberOfQueries != null ? numberOfQueries.hashCode() : 0);
        result = 31 * result + (numberOfResults != null ? numberOfResults.hashCode() : 0);
        result = 31 * result + (startupTime != null ? startupTime.hashCode() : 0);
        return result;
    }
}
