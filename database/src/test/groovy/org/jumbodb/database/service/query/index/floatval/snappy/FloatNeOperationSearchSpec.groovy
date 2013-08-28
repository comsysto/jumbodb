package org.jumbodb.database.service.query.index.floatval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class FloatNeOperationSearchSpec extends Specification {
    def operation = new FloatNeOperationSearch()

    @Unroll
    def "not equal match #value != #testValue == #isNotEqual"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.NE, value)

        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isNotEqual
        where:
        value | testValue | isNotEqual
        5f    | 5f        | false
        5f    | 4.99f     | true
        5f    | 5.01f     | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchDate with expected chunk #expectedChunk"() {
        setup:
        def file = FloatDataGeneration.createFile();
        def snappyChunks = FloatDataGeneration.createIndexFile(file)
        def retriever = FloatDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new QueryClause(QueryOperation.NE, searchDate)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchDate | expectedChunk
        -2050f      | 0 // is outside of the generated range
        -2047f      | 0
        -1f         | 0
        0f          | 0
        1f          | 0
        2048f       | 0   // is equal to starting value has to check a chunk before
        2049f       | 0
        16385f      | 0
        18432f      | 0 // is equal to starting value has to check a chunk before
        18433f      | 0
        20479f      | 0
        22000f      | 0 // is outside of the generated rang
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.NE, queryValue)
        def indexFile = new NumberSnappyIndexFile<Float>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99f      | 1f            | 11f         | true
        1f         | 1f            | 11f         | true
        1.01f      | 1f            | 11f         | true
        10.99f     | 1f            | 11f         | true
        11f        | 1f            | 11f         | true
        11.01f     | 1f            | 11f         | true
        1f         | 1f            | 1f          | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.NE, 5f))
        then:
        valueRetriever instanceof FloatQueryValueRetriever
    }
}