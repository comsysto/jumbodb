package org.jumbodb.database.service.query.index.common.floatval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class FloatGtEqOperationSearchSpec extends Specification {
    def operation = new FloatGtEqOperationSearch()

    @Unroll
    def "greater match #value <= #testValue == #isGreater"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.GT_EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isGreater
        where:
        value | testValue | isGreater
        5f    | 5f        | true
        5f    | 5.01f     | true
        5f    | 4.99f     | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = FloatDataGeneration.createFile();
        def snappyChunks = FloatDataGeneration.createIndexFile(file)
        def retriever = FloatDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingBlock(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.GT_EQ, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchValue | expectedChunk
        -2050f      | 0 // is outside of the generated range
        -2047f      | 0
        -1f         | 0
        0f          | 0
        1f          | 1
        2048f       | 1   // is equal to starting value has to check a chunk before
        2049f       | 2
        16385f      | 9
        18432f      | 9 // is equal to starting value has to check a chunk before
        18433f      | 10
        20479f      | 10
        22000f      | 10 // is outside of the generated rang
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.GT_EQ, queryValue)
        def indexFile = new NumberIndexFile<Float>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99f      | 1f            | 11f         | true
        1f         | 1f            | 11f         | true
        1.01f      | 1f            | 11f         | true
        11f        | 1f            | 11f         | true
        10.99f     | 1f            | 11f         | true
        11.01f     | 1f            | 11f         | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.GT_EQ, 5f))
        then:
        valueRetriever instanceof FloatQueryValueRetriever
    }


}