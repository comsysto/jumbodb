package org.jumbodb.database.service.query.index.common.floatval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class FloatLtEqOperationSearchSpec extends Specification {
    def operation = new FloatLtEqOperationSearch()

    @Unroll
    def "less match #value >= #testValue == #isLess"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.LT_EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isLess
        where:
        value | testValue | isLess
        2f    | 2f        | true
        2f    | 2.01f     | false
        2f    | 1.99f     | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = FloatDataGeneration.createFile();
        def snappyChunks = FloatDataGeneration.createIndexFile(file)
        def retriever = FloatDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingBlock(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.LT_EQ, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchValue | expectedChunk
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
        def indexQuery = new IndexQuery("testIndex", QueryOperation.LT_EQ, queryValue)
        def indexFile = new NumberIndexFile<Float>(indexFileFrom, indexFileTo, Mock(File))
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99f      | 1f            | 11f         | false
        1f         | 1f            | 11f         | true
        1.01f      | 1f            | 11f         | true
        10.99f     | 1f            | 11f         | true
        11f        | 1f            | 11f         | true
        11.01f     | 1f            | 11f         | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.LT_EQ, 5f))
        then:
        valueRetriever instanceof FloatQueryValueRetriever
    }
}