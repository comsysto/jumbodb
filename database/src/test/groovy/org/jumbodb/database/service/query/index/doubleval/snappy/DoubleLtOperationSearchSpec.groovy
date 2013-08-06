package org.jumbodb.database.service.query.index.doubleval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

import java.text.SimpleDateFormat

/**
 * @author Carsten Hufe
 */
class DoubleLtOperationSearchSpec extends Specification {
    def operation = new DoubleLtOperationSearch(new DoubleSnappyIndexStrategy())

    @Unroll
    def "less match #value > #testValue == #isLess"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, value)
        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isLess
        where:
        value | testValue | isLess
        2d    | 2d        | false
        2d    | 2.01d     | false
        2d    | 1.99d     | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = DoubleDataGeneration.createFile();
        def snappyChunks = DoubleDataGeneration.createIndexFile(file)
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.LT, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
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
        def queryClause = new QueryClause(QueryOperation.LT, queryValue)
        def indexFile = new NumberSnappyIndexFile<Double>(indexFileFrom, indexFileTo, Mock(File))
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0.99d      | 1d            | 11d         | false
        1d         | 1d            | 11d         | false
        1.01d      | 1d            | 11d         | true
        10.99d     | 1d            | 11d         | true
        11d        | 1d            | 11d         | true
        11.01d     | 1d            | 11d         | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.LT, 5d))
        then:
        valueRetriever instanceof DoubleQueryValueRetriever
    }
}