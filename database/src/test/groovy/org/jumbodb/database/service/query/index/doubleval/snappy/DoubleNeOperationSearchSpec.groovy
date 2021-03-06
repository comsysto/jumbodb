package org.jumbodb.database.service.query.index.doubleval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll


/**
 * @author Carsten Hufe
 */
class DoubleNeOperationSearchSpec extends Specification {
    def operation = new DoubleNeOperationSearch()

    @Unroll
    def "not equal match #value != #testValue == #isNotEqual"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.NE, value)

        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isNotEqual
        where:
        value | testValue | isNotEqual
        5d    | 5d        | false
        5d    | 4.99d     | true
        5d    | 5.01d     | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchDate with expected chunk #expectedChunk"() {
        setup:
        def file = DoubleDataGeneration.createFile();
        def snappyChunks = DoubleDataGeneration.createIndexFile(file)
        def retriever = DoubleDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new QueryClause(QueryOperation.NE, searchDate)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchDate | expectedChunk
        -1700d     | 0 // is outside of the generated range
        -1599d     | 0
        -1d        | 0
        0d         | 0
        1d         | 0
        1600d      | 0   // is equal to starting value has to check a chunk before
        1601d      | 0
        12801d     | 0
        14400d     | 0 // is equal to starting value has to check a chunk before
        14401d     | 0
        15999d     | 0
        20000d     | 0 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.NE, queryValue)
        def indexFile = new NumberSnappyIndexFile<Double>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99d      | 1d            | 11d         | true
        1d         | 1d            | 11d         | true
        1.01d      | 1d            | 11d         | true
        10.99d     | 1d            | 11d         | true
        11d        | 1d            | 11d         | true
        11.01d     | 1d            | 11d         | true
        1d         | 1d            | 1d          | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.NE, 5d))
        then:
        valueRetriever instanceof DoubleQueryValueRetriever
    }
}