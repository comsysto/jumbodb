package org.jumbodb.database.service.query.index.common.datetime

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import org.jumbodb.database.service.query.index.snappy.DateTimeSnappyDataGeneration
import spock.lang.Specification
import spock.lang.Unroll

import java.text.SimpleDateFormat

/**
 * @author Carsten Hufe
 */
class DateTimeLtEqOperationSearchSpec extends Specification {
    def operation = new DateTimeLtEqOperationSearch()

    @Unroll
    def "less match #value >= #testValue == #isLess"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.LT_EQ, value)
        def sdf = new SimpleDateFormat(DateTimeQueryValueRetriever.DATE_SEARCH_PATTERN)

        operation.matching(sdf.parse(testValue).getTime(), operation.getQueryValueRetriever(indexQuery)) == isLess
        where:
        value                 | testValue             | isLess
        "2012-10-01 12:00:00" | "2012-10-01 12:00:00" | true
        "2012-10-01 12:00:00" | "2012-10-01 12:00:01" | false
        "2012-10-01 12:00:00" | "2012-10-01 11:59:59" | true
    }

    @Unroll
    def "findFirstMatchingChunk #searchDate with expected chunk #expectedChunk"() {
        setup:
        def file = DateTimeSnappyDataGeneration.createFile();
        def snappyChunks = DateTimeSnappyDataGeneration.createIndexFile(file)
        def retriever = DateTimeSnappyDataGeneration.createFileDataRetriever(file, snappyChunks)
        expect:
        operation.findFirstMatchingBlock(retriever, operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.LT_EQ, searchDate)), snappyChunks) == expectedChunk
        cleanup:
        file.delete();
        where:
        searchDate            | expectedChunk
        "2012-01-01 06:00:00" | 0 // is outside of the generated range
        "2012-01-01 12:00:00" | 0
        "2012-01-30 12:00:00" | 0
        "2012-02-01 12:00:00" | 0   // is equal to starting value has to check a chunk before
        "2012-02-01 12:00:01" | 0
        "2012-11-15 12:00:00" | 0
        "2012-12-01 12:00:00" | 0 // is equal to starting value has to check a chunk before
        "2012-12-01 12:00:01" | 0
        "2012-12-28 12:00:00" | 0
        "2012-12-30 12:00:00" | 0 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.LT_EQ, queryValue)
        def sdf = new SimpleDateFormat(DateTimeQueryValueRetriever.DATE_SEARCH_PATTERN)
        def indexFile = new NumberIndexFile<Long>(sdf.parse(indexFileFrom).getTime(), sdf.parse(indexFileTo).getTime(), Mock(File))
        operation.acceptIndexFile(operation.getQueryValueRetriever(indexQuery), indexFile) == accept
        where:
        queryValue            | indexFileFrom         | indexFileTo           | accept
        "2012-10-01 12:00:00" | "2012-11-01 12:00:01" | "2013-10-01 11:59:59" | false
        "2012-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 11:59:59" | true
        "2012-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | true
        "2012-10-01 12:00:00" | "2012-10-01 12:00:01" | "2013-10-01 12:00:00" | false
        "2013-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 11:59:59" | true
        "2013-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | true
        "2013-10-01 11:59:59" | "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | true
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.LT_EQ, "2013-10-01 11:59:59"))
        then:
        valueRetriever instanceof DateTimeQueryValueRetriever
    }
}