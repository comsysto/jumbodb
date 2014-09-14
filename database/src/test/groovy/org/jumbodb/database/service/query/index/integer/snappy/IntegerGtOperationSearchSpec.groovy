package org.jumbodb.database.service.query.index.integer.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class IntegerGtOperationSearchSpec extends Specification {
    def operation = new IntegerGtOperationSearch()

    @Unroll
    def "greater match #value < #testValue == #isGreater"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.GT, value)
        operation.matching(testValue, operation.getQueryValueRetriever(indexQuery)) == isGreater
        where:
        value | testValue | isGreater
        5     | 5         | false
        5     | 6         | true
        5     | 4         | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchValue with expected chunk #expectedChunk"() {
        setup:
        def file = IntegerDataGeneration.createFile();
        def snappyChunks = IntegerDataGeneration.createIndexFile(file)
        def retriever = IntegerDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingChunk(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.GT, searchValue)), snappyChunks) == expectedChunk
        cleanup:
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
        def queryCindexQueryause = new IndexQuery("testIndex", QueryOperation.GT, queryValue)
        def indexFile = new NumberSnappyIndexFile<Integer>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryCindexQueryause), indexFile) == accept
        where:
        queryValue | indexFileFrom | indexFileTo | accept
        0          | 1             | 11          | true
        1          | 1             | 11          | true
        1          | 1             | 11          | true
        11         | 1             | 11          | false
        10         | 1             | 11          | true
        12         | 1             | 11          | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.GT, 5))
        then:
        valueRetriever instanceof IntegerQueryValueRetriever
    }
}