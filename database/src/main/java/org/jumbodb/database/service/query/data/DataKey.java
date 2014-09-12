package org.jumbodb.database.service.query.data;


public class DataKey {

    private final String chunkKey;
    private final String collectionName;


    public DataKey(String chunkKey, String collectionName) {
        this.collectionName = collectionName;
        this.chunkKey = chunkKey;
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
