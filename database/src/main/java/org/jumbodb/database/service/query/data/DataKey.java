package org.jumbodb.database.service.query.data;


public class DataKey {

    private final String collectionName;
    private final String chunkKey;


    public DataKey(String collectionName, String chunkKey) {
        this.chunkKey = chunkKey;
        this.collectionName = collectionName;
    }


    public String getCollectionName() {
        return collectionName;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataKey that = (DataKey) o;

        if (!chunkKey.equals(that.chunkKey)) return false;
        if (!collectionName.equals(that.collectionName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = collectionName.hashCode();
        result = 31 * result + chunkKey.hashCode();
        return result;
    }
}
