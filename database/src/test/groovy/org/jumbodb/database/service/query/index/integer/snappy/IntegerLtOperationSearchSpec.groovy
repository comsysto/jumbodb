package org.jumbodb.database.service.query.index.integer.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class IntegerLtOperationSearchSpec extends Specification {
    def operation = new IntegerLtOperationSearch()

    @Unroll
    def "less match #value > #testValue == #isLess"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, value)
        operation.matching(testValue, operation.getQueryValueRetriever(queryClause)) == isLess
        where:
        value | testValue | isLess
        2     | 2         | false
        2     | 3         | false
        2     | 1         | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = IntegerDataGeneration.createFile();
        def snappyChunks = IntegerDataGeneration.createIndexFile(file)
        def retriever = IntegerDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new QueryClause(QueryOperation.LT, searchValue)), snappyChunks) == expectedChunk
        cleanup:
//        ramFile.close()
        file.delete();
        where:
        searchValue | expectedChunk
        -2050       | 0 // is outside of the generated range
        -2047       | 0
        -1          | 0
        0           | 0
        1           | 0
        2048        | 0   // is equal to starting value has to check a chunk before
        2049        | 0
        16385       | 0
        18432       | 0 // is equal to starting value has to check a chunk before
        18433       | 0
        20479       | 0
        22000       | 0 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.LT, queryValue)
        def indexFile = new NumberSnappyIndexFile<Integer>(indexFileFrom, indexFileTo, Mock(File))
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0          | 1             | 11          | false
        1          | 1             | 11          | false
        2          | 1             | 11          | true
        10         | 1             | 11          | true
        11         | 1             | 11          | true
        12         | 1             | 11          | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.LT, 5))
        then:
        valueRetriever instanceof IntegerQueryValueRetriever
    }
}