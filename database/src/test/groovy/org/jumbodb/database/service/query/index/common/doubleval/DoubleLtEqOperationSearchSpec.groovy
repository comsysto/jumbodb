package org.jumbodb.database.service.query.index.common.doubleval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DoubleLtEqOperationSearchSpec extends Specification {
    def operation = new DoubleLtEqOperationSearch()

    @Unroll
    def "less match #value >= #testValue == #isLess"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.LT_EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isLess
        where:
        value | testValue | isLess
        2d    | 2d        | true
        2d    | 1.99d     | true
        2d    | 2.01d     | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = DoubleDataGeneration.createFile();
        def snappyChunks = DoubleDataGeneration.createIndexFile(file)
        def retriever = DoubleDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingBlock(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.LT_EQ, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchValue | expectedChunk
        -1700d      | 0 // is outside of the generated range
        -1599d      | 0
        -1d         | 0
        0d          | 0
        1d          | 0
        1600d       | 0   // is equal to starting value has to check a chunk before
        1601d       | 0
        12801d      | 0
        14400d      | 0 // is equal to starting value has to check a chunk before
        14401d      | 0
        15999d      | 0
        20000d      | 0 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.LT_EQ, queryValue)
        def indexFile = new NumberIndexFile<Double>(indexFileFrom, indexFileTo, Mock(File))
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99d      | 1d            | 11d         | false
        1d         | 1d            | 11d         | true
        1.01d      | 1d            | 11d         | true
        10.99d     | 1d            | 11d         | true
        11d        | 1d            | 11d         | true
        11.01d     | 1d            | 11d         | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.LT_EQ, 5d))
        then:
        valueRetriever instanceof DoubleQueryValueRetriever
    }
}