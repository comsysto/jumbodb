package org.jumbodb.database.service.query.index.integer.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class IntegerEqOperationSearchSpec extends Specification {
    def operation = new IntegerEqOperationSearch(new IntegerSnappyIndexStrategy())

    @Unroll
    def "equal match #value == #testValue == #isEqual"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.EQ, value)
        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isEqual
        where:
        value | testValue | isEqual
        -123  | -123      | true
        123   | 123       | true
        123   | 124       | false
        123   | 122       | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = IntegerDataGeneration.createFile();
        def snappyChunks = IntegerDataGeneration.createIndexFile(file)
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.EQ, searchValue)), snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
        file.delete();
        where:
        searchValue | expectedChunk
        -2050       | 0 // is outside of the generated range
        -2047       | 0
        -1          | 0
        0           | 0
        1           | 1
        2048        | 1   // is equal to starting value has to check a chunk before
        2049        | 2
        16385       | 9
        18432       | 9 // is equal to starting value has to check a chunk before
        18433       | 10
        20479       | 10
        22000       | 10 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.EQ, queryValue)
        def indexFile = new NumberSnappyIndexFile<Integer>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0          | 1             | 11          | false
        1          | 1             | 11          | true
        2          | 1             | 11          | true
        10         | 1             | 11          | true
        11         | 1             | 11          | true
        12         | 1             | 11          | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.EQ, 5))
        then:
        valueRetriever instanceof IntegerQueryValueRetriever
    }
}