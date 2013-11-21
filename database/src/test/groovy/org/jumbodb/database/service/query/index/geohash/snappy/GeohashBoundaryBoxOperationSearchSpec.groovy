package org.jumbodb.database.service.query.index.geohash.snappy

import org.jumbodb.common.geo.geohash.GeoHash
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class GeohashBoundaryBoxOperationSearchSpec extends Specification {
    def operation = new GeohashBoundaryBoxOperationSearch()

    @Unroll
    def "equal match #value == #testValue == #isEqual"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, value)
        def geohash = GeoHash.withBitPrecision(testValue[0], testValue[1], 32).intValue()
        operation.matching(new GeohashCoords(geohash, testValue[0], testValue[1]), operation.getQueryValueRetriever(queryClause)) == isEqual
        where:
        value                                            | testValue                               | isEqual
        [[48.207688, 11.331185], [48.215382, 11.352847]] | [48.208416, 11.332958]                  | true  // olching
        [[48.207688, 11.331185], [48.215382, 11.352847]] | [48.200000, 11.332958]                  | false // olching
        [[48.207688, 11.331185], [48.215382, 11.352847]] | [48.208416, 11.330000]                  | false // olching
        [[51.516652, -0.131793], [51.586833, 0.077033]]  | [51.536652, -0.010000]                  | true // london
        [[51.516652, -0.131793], [51.586833, 0.077033]]  | [51.536652, 0.0100000]                  | true // london
        [[51.516652, -0.131793], [51.586833, 0.077033]]  | [51.536652, 0.0000000]                  | true // london
        [[51.516652, -0.131793], [51.586833, 0.077033]]  | [51.536652, 0.1000000]                  | false // london
        [[51.516652, -0.131793], [51.586833, 0.077033]]  | [52.536652, 0.0000000]                  | false // london
        [[51.542278, -0.119877], [51.577070, -0.027180]] | [51.5431421180649, -0.111531789001861]  | true // london
        [[51.542278, -0.119877], [51.577070, -0.027180]] | [51.5744836835149, -0.0705851243789901] | true // london
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = GeohashDataGeneration.createFile();
        def snappyChunks = GeohashDataGeneration.createIndexFile(file)
        def retriever = GeohashDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        file.delete()
        where:
        searchValue                    | expectedChunk
        [[1.0, 0.0], [1.0, 0.0]]       | 0 // is outside of the generated range
        [[1.0, 0.01], [1.0, 0.01]]     | 0
        [[1.0, 20.48], [1.0, 20.48]]   | 7
        [[2.0, 20.48], [2.0, 20.48]]   | 7
        [[1.0, 20.48], [2.0, 20.48]]   | 7
        [[5.0, 5.0], [5.1, 5.1]]       | 1
        [[1.0, 20.48], [3.0, 20.48]]   | 6
        [[9.9, 20.48], [9.9, 20.48]]   | 9
        [[9.9, 20.48], [12.0, 20.48]]  | 0
        [[10.0, 20.48], [10.0, 20.48]] | 9
        [[10.0, 20.48], [11.0, 20.48]] | 9 // outside
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, queryValue)
        def indexFile = new NumberSnappyIndexFile<Integer>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue                   | indexFileFrom | indexFileTo | accept
        [[1.0, 0.00], [1.0, 0.00]]   | -1073671086   | -1062627760 | false
        [[0.9, 0.01], [0.9, 0.01]]   | -1073671086   | -1062627760 | false
        [[0.9, 0.01], [1.1, 0.01]]   | -1073671086   | -1062627760 | true
        [[1.0, 0.01], [1.0, 0.01]]   | -1073671086   | -1062627760 | true
        [[1.0, 20.48], [1.0, 20.47]] | -1073671086   | -1062627760 | true
        [[1.0, 20.48], [1.0, 20.48]] | -1073671086   | -1062627760 | true
        [[1.0, 20.49], [1.0, 20.49]] | -1073671086   | -1062627760 | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, [[1f, 2f], [3f, 4f]]))
        then:
        valueRetriever instanceof GeohashQueryValueRetriever
    }
}