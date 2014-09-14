package org.jumbodb.database.service.query.index.doubleval.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DoubleEqOperationSearchSpec extends Specification {
    def operation = new DoubleEqOperationSearch()

    @Unroll
    def "equal match #value == #testValue == #isEqual"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isEqual
        where:
        value    | testValue  | isEqual
        33.333d  | 33.333d    | true
        4444.44d | 4444.44d   | true
        4444.44d | 4444.441d  | false
        4444.44d | 4444.4399d | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = DoubleDataGeneration.createFile();
        def snappyChunks = DoubleDataGeneration.createIndexFile(file)
        def retriever = DoubleDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, searchValue)), snappyChunks) == expectedChunk
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
        def indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, queryValue)
        def indexFile = new NumberSnappyIndexFile<Double>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99d      | 1d            | 11d         | false
        1d         | 1d            | 11d         | true
        1d         | 0.99d         | 11d         | true
        11d        | 1d            | 11d         | true
        11.01d     | 1d            | 11d         | false
        5d         | 1d            | 11d         | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, 5d))
        then:
        valueRetriever instanceof DoubleQueryValueRetriever
    }
}