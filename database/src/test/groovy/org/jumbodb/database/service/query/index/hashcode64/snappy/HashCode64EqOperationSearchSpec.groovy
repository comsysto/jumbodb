package org.jumbodb.database.service.query.index.hashcode64.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever
import org.jumbodb.database.service.query.index.longval.snappy.LongDataGeneration
import org.jumbodb.database.service.query.index.longval.snappy.LongEqOperationSearch
import org.jumbodb.database.service.query.index.longval.snappy.LongQueryValueRetriever
import org.jumbodb.database.service.query.index.longval.snappy.LongSnappyIndexStrategy
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class HashCode64EqOperationSearchSpec extends Specification {
    def operation = new HashCode64EqOperationSearch(new HashCode64SnappyIndexStrategy())

    @Unroll
    def "equal match #hashCodeValue == #testValue == #isEqual"() {
        setup:
        def queryRetrieverMock = Mock(QueryValueRetriever)
        queryRetrieverMock.getValue() >> hashCodeValue
        expect:
        operation.matching(testValue, queryRetrieverMock) == isEqual
        where:
        hashCodeValue | testValue | isEqual
        -123l         | -123l     | true
        123l          | 123l      | true
        123l          | 124l      | false
        123l          | 122l      | false
    }

    @Unroll
    def "findFirstMatchingChunk #hashCodeValue with expected chunk #expectedChunk"() {
        setup:
        def file = LongDataGeneration.createFile();
        def snappyChunks = LongDataGeneration.createIndexFile(file)
        def ramFile = new RandomAccessFile(file, "r")
        def queryRetrieverMock = Mock(QueryValueRetriever)
        queryRetrieverMock.getValue() >> hashCodeValue
        expect:
        operation.findFirstMatchingChunk(ramFile, queryRetrieverMock, snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
        file.delete();
        where:
        hashCodeValue | expectedChunk
        -1700l        | 0 // is outside of the generated range
        -1599l        | 0
        -1l           | 0
        0l            | 0
        1l            | 1
        1600l         | 1   // is equal to starting value has to check a chunk before
        1601l         | 2
        12801l        | 9
        14400l        | 9 // is equal to starting value has to check a chunk before
        14401l        | 10
        15999l        | 10
        20000l        | 10 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile hashCodeValue=#hashCodeValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        setup:
        def queryRetrieverMock = Mock(QueryValueRetriever)
        queryRetrieverMock.getValue() >> hashCodeValue
        expect:
        def indexFile = new NumberSnappyIndexFile<Long>(indexFileFrom, indexFileTo, Mock(File));
        operation.acceptIndexFile(queryRetrieverMock, indexFile) == accept
        where:
        hashCodeValue | indexFileFrom | indexFileTo | accept
        0l            | 1l            | 11l         | false
        1l            | 1l            | 11l         | true
        2l            | 1l            | 11l         | true
        10l           | 1l            | 11l         | true
        11l           | 1l            | 11l         | true
        12l           | 1l            | 11l         | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.EQ, 5l))
        then:
        valueRetriever instanceof HashCode64QueryValueRetriever
    }
}