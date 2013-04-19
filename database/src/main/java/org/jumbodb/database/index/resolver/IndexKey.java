package org.jumbodb.database.index.resolver;


public class IndexKey {

    private final String collectionName;
    private final String chunkeKey;
    private final String indexName;


    public IndexKey(String indexName, String chunkeKey, String collectionName) {
        this.indexName = indexName;
        this.chunkeKey = chunkeKey;
        this.collectionName = collectionName;
    }


    public String getCollectionName() {
        return collectionName;
    }

    public String getChunkeKey() {
        return chunkeKey;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexKey that = (IndexKey) o;

        if (!chunkeKey.equals(that.chunkeKey)) return false;
        if (!collectionName.equals(that.collectionName)) return false;
        if (!indexName.equals(that.indexName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = collectionName.hashCode();
        result = 31 * result + chunkeKey.hashCode();
        result = 31 * result + indexName.hashCode();
        return result;
    }
}
