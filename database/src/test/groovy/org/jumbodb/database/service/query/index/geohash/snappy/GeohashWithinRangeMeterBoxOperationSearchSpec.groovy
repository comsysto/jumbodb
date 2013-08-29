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
class GeohashWithinRangeMeterBoxOperationSearchSpec extends Specification {
    def operation = new GeohashWithinRangeMeterBoxOperationSearch()

    @Unroll
    def "equal match #value == #testValue == #isEqual"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, value)
        def geohash = GeoHash.withBitPrecision(testValue[0], testValue[1], 32).intValue()
        operation.matching(new GeohashCoords(geohash, testValue[0], testValue[1]), operation.getQueryValueRetriever(queryClause)) == isEqual
        where:
        value                          | testValue              | isEqual
        [[48.207688, 11.331185], 1]    | [48.208416, 11.332958] | false  // olching
        [[48.207688, 11.331185], 864]  | [48.200000, 11.332958] | false // olching
        [[48.207688, 11.331185], 865]  | [48.200000, 11.332958] | true // olching
        [[48.207688, 11.331185], 1000] | [48.200000, 11.332958] | true // olching
        // FIXME london problem with zero meridian
//        [[51.516652, -0.131793], 100]  | [51.536652, -0.010000] | true // london
////        [[51.516652, -0.131793], 1000]  | [51.536652, 0.0100000] | true // london
////        [[51.516652, -0.131793], 1000]  | [51.536652, 0.0000000] | true // london
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = GeohashDataGeneration.createFile();
        def snappyChunks = GeohashDataGeneration.createIndexFile(file)
        def retriever = GeohashDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        file.delete()
        where:
        searchValue            | expectedChunk
        [[1.0, 0.0], 1]        | 0 // is outside of the generated range
        [[1.0, 0.01], 1]       | 0
        [[1.0, 20.48], 1]      | 7
        [[2.0, 20.48], 1]      | 7
        [[1.0, 20.48], 1]      | 7
        [[5.0, 5.0], 1]        | 1
        [[1.0, 20.48], 200000] | 6
        [[9.9, 20.48], 1]      | 9
        [[9.9, 20.48], 200000] | 9
        [[10.0, 20.48], 1]     | 9
        [[10.0, 20.48], 1]     | 9 // outside
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, queryValue)
        def indexFile = new NumberSnappyIndexFile<Integer>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue             | indexFileFrom | indexFileTo | accept
        [[1.0, 0.00], 0]       | -1073671086   | -1062627760 | false
        [[0.9, 0.01], 1]       | -1073671086   | -1062627760 | false
        [[0.9, 0.01], 200000]  | -1073671086   | -1062627760 | true
        [[1.0, 0.01], 1]       | -1073671086   | -1062627760 | true
        [[1.0, 20.48], 1]      | -1073671086   | -1062627760 | true
        [[1.0, 20.48], 1]      | -1073671086   | -1062627760 | true
        [[1.0, 20.49], 1]      | -1073671086   | -1062627760 | false
        [[9.9, 20.48], 200000] | -1073671086   | -1062627760 | false

    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, [[1f, 2f], 5]))
        then:
        valueRetriever instanceof GeohashWithingRangeMeterQueryValueRetriever
    }
}