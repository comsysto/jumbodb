package org.jumbodb.database.service.management.storage

/**
 * @author Carsten Hufe
 */
class StorageManagementSpec extends spock.lang.Specification {

    def "findQueryableCollections should return two queryable collections"() {
        // TODO
    }

    def "deleteCompleteCollection should delete a collection with all versions and chunks"() {
        // TODO
    }

    def "deleteChunkedVersionForAllCollections should delete all collections inside the delivery version"() {
        // TODO
    }

    def "deleteChunkedVersionInCollection should delete only a chunked version for the collection"() {
        // TODO
    }

    def "activateChunkedVersionForAllCollections should activate the version for all available collections"() {
        // TODO
    }

    def "activateChunkedVersionInCollection should activate the version for the given collection"() {
        // TODO
    }

    def "getActiveDeliveryVersion should return the active version in a collection"() {
        // TODO
    }

    def "getJumboCollections should return all jumbo collections"() {
        // TODO
    }

    def "getJumboCollection should return information for the given jumbo collection"() {
        // TODO
    }

    def "getChunkedDeliveryVersions should return all deliveries with collections"() {
        // TODO
    }

    def "getMetaIndexForDelivery should return the delivery meta data"() {
        // TODO
    }

    def "getIndexInfoForDelivery should map to IndexInfo and add sizes"() {
        // TODO
    }

    def "getDataInfoForDelivery should map to DataInfo and add sizes"() {
        // TODO
    }

    def "getInputStream(IndexInfo) should open a input stream to the given index file"() {
        // TODO
    }

    def "getInputStream(DataInfo) should open a input stream to the given data file"() {
        // TODO
    }
}
