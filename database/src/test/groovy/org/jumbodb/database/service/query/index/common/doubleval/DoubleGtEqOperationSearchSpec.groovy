package org.jumbodb.database.service.query.index.common.doubleval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DoubleGtEqOperationSearchSpec extends Specification {
    def operation = new DoubleGtEqOperationSearch()

    @Unroll
    def "greater match #value <= #testValue == #isGreater"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.GT_EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isGreater
        where:
        value | testValue | isGreater
        5d    | 5d        | true
        5d    | 5.01d     | true
        5d    | 4.99d     | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = DoubleDataGeneration.createFile();
        def snappyChunks = DoubleDataGeneration.createIndexFile(file)
        def retriever = DoubleDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingBlock(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.GT_EQ, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchValue | expectedChunk
        -1700d      | 0 // is outside of the generated range
        -1599d      | 0
        -1d         | 0
        0d          | 0
        1d          | 1
        1600d       | 1   // is equal to starting value has to check a chunk before
        1601d       | 2
        12801d      | 9
        14400d      | 9 // is equal to starting value has to check a chunk before
        14401d      | 10
        15999d      | 10
        20000d      | 10 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new IndexQuery("testIndex", QueryOperation.GT_EQ, queryValue)
        def indexFile = new NumberIndexFile<Double>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99d      | 1d            | 11d         | true
        1d         | 1d            | 11d         | true
        1.01d      | 1d            | 11d         | true
        11d        | 1d            | 11d         | true
        10.99d     | 1d            | 11d         | true
        11.01d     | 1d            | 11d         | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.GT_EQ, 5d))
        then:
        valueRetriever instanceof DoubleQueryValueRetriever
    }


}