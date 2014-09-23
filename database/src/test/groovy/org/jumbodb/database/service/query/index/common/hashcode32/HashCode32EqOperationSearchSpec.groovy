package org.jumbodb.database.service.query.index.common.hashcode32

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import org.jumbodb.database.service.query.index.common.QueryValueRetriever
import org.jumbodb.database.service.query.index.snappy.HashCode32SnappyDataGeneration
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class HashCode32EqOperationSearchSpec extends Specification {
    def operation = new HashCode32EqOperationSearch()

    @Unroll
    def "equal match #hashCodeValue == #testValue == #isEqual"() {
        setup:
        def queryRetrieverMock = Mock(QueryValueRetriever)
        queryRetrieverMock.getValue() >> hashCodeValue
        expect:
        operation.matching(testValue, queryRetrieverMock) == isEqual
        where:
        hashCodeValue | testValue | isEqual
        -123          | -123      | true
        123           | 123       | true
        123           | 124       | false
        123           | 122       | false
    }

    @Unroll
    def "findFirstMatchingChunk #hashCodeValue with expected chunk #expectedChunk"() {
        setup:
        def file = HashCode32SnappyDataGeneration.createFile();
        def snappyChunks = HashCode32SnappyDataGeneration.createIndexFile(file)
        def retriever = HashCode32SnappyDataGeneration.createFileDataRetriever(file, snappyChunks)
        def queryRetrieverMock = Mock(QueryValueRetriever)
        queryRetrieverMock.getValue() >> hashCodeValue
        expect:
        operation.findFirstMatchingBlock(retriever, queryRetrieverMock, snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        hashCodeValue | expectedChunk
        -2050         | 0 // is outside of the generated range
        -2047         | 0
        -1            | 0
        0             | 0
        1             | 1
        2048          | 1   // is equal to starting value has to check a chunk before
        2049          | 2
        16385         | 9
        18432         | 9 // is equal to starting value has to check a chunk before
        18433         | 10
        20479         | 10
        22000         | 10 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile hashCodeValue=#hashCodeValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        setup:
        def queryRetrieverMock = Mock(QueryValueRetriever)
        queryRetrieverMock.getValue() >> hashCodeValue
        expect:
        def indexFile = new NumberIndexFile<Integer>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(queryRetrieverMock, indexFile) == accept
        where:
        hashCodeValue | indexFileFrom | indexFileTo | accept
        0             | 1             | 11          | false
        1             | 1             | 11          | true
        2             | 1             | 11          | true
        10            | 1             | 11          | true
        11            | 1             | 11          | true
        12            | 1             | 11          | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, "What ever"))
        then:
        valueRetriever instanceof HashCode32QueryValueRetriever
    }
}