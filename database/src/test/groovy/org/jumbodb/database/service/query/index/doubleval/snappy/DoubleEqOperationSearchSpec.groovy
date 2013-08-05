package org.jumbodb.database.service.query.index.doubleval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeDataGeneration
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeEqOperationSearch
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeQueryValueRetriever
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeSnappyIndexStrategy
import spock.lang.Specification
import spock.lang.Unroll

import java.text.SimpleDateFormat

/**
 * @author Carsten Hufe
 */
class DoubleEqOperationSearchSpec extends Specification {
    def operation = new DoubleEqOperationSearch(new DoubleSnappyIndexStrategy())

    @Unroll
    def "equal match #value == #testValue == #isEqual"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isEqual
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
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.EQ, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
        file.delete();
        where:
        searchValue | expectedChunk
        -1d         | 0 // is outside of the generated range
        0d          | 0
        1d          | 0
        1600d       | 0   // is equal to starting value has to check a chunk before
        1601d       | 1
        12801d      | 8
        14400d      | 8 // is equal to starting value has to check a chunk before
        14401d      | 9
        15999d      | 9
        20000d      | 9 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.EQ, queryValue)
        def indexFile = new NumberSnappyIndexFile<Double>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
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
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.EQ, 5d))
        then:
        valueRetriever instanceof DoubleQueryValueRetriever
    }
}