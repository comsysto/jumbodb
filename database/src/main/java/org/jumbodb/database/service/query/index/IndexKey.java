package org.jumbodb.database.service.query.index;


public class IndexKey {

    private final String chunkKey;
    private final String collectionName;
    private final String indexName;


    public IndexKey(String chunkKey, String collectionName, String indexName) {
        this.chunkKey = chunkKey;
        this.collectionName = collectionName;
        this.indexName = indexName;
    }


    public String getCollectionName() {
        return collectionName;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexKey that = (IndexKey) o;

        if (!chunkKey.equals(that.chunkKey)) return false;
        if (!collectionName.equals(that.collectionName)) return false;
        if (!indexName.equals(that.indexName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = collectionName.hashCode();
        result = 31 * result + chunkKey.hashCode();
        result = 31 * result + indexName.hashCode();
        return result;
    }
}
