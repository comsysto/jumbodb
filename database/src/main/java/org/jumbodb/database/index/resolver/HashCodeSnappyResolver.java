package org.jumbodb.database.index.resolver;

import org.jumbodb.database.service.query.FileOffset;

import java.util.List;

/**
 * @author ugitsch
 */
public class HashCodeSnappyResolver extends IndexResolver{

    enum QueryOperation {

        EQ,
        GT,
        LT,
        NE
    }


    // FileOffset -> ein Zeilen-Offset pro File
    // { filename hash: "1234", offset: 55000" } => 55000 werden geskipped, das kommt schon von den IndexDateien
    // { filename hash: "1234", offset: 80099" } => 80099
    // ==> würden 2 Resulsts ergeben, unsortiert... Werden später gruppiert
    List<FileOffset> findFileOffsets(String collection, String chunkName, String indexName, Query query ) {

        return null;
    }

    public boolean isResponsibleFor(String collection, String chunkName, String indexName) {

        return false;
    }


    boolean supports(QueryOperation operation) {

        return true;
    }

    String getProviderName() {
        return "HASH_CODE_SNAPPY_V1";
    }


    // notification
    public void onDataChanged() {

    }


    private static class Query{}
}
