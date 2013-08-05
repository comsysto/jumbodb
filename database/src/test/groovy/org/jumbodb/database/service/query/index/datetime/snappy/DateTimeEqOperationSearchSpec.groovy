package org.jumbodb.database.service.query.index.datetime.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Specification
import spock.lang.Unroll

import java.text.SimpleDateFormat

/**
 * @author Carsten Hufe
 */
class DateTimeEqOperationSearchSpec extends Specification {
    def operation = new DateTimeEqOperationSearch(new DateTimeSnappyIndexStrategy())

    @Unroll
    def "equal match #value == #testValue == #isEqual"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.EQ, value)
        def sdf = new SimpleDateFormat(DateTimeQueryValueRetriever.DATE_SEARCH_PATTERN)

        operation.matching(sdf.parse(testValue).getTime(), operation.getQueryValueRetriever(queryClause)) == isEqual
        where:
        value                 | testValue             | isEqual
        "2012-10-01 12:00:00" | "2012-10-01 12:00:00" | true
        "2012-10-01 12:00:01" | "2012-10-01 12:00:01" | true
        "2012-10-01 12:00:00" | "2012-10-01 12:00:01" | false
        "2012-10-01 12:00:00" | "2012-10-01 11:59:59" | false
    }

    @Unroll
    def "findFirstMatchingChunk #searchDate with expected chunk #expectedChunk"() {
        setup:
        def file = DateTimeDataGeneration.createFile();
        def snappyChunks = DateTimeDataGeneration.createIndexFile(file)
        def ramFile = new RandomAccessFile(file, "r")
        expect:
        operation.findFirstMatchingChunk(ramFile, operation.getQueryValueRetriever(new QueryClause(QueryOperation.EQ, searchDate)), snappyChunks) == expectedChunk
        cleanup:
        ramFile.close()
        file.delete();
        where:
        searchDate            | expectedChunk
        "2012-01-01 06:00:00" | 0 // is outside of the generated range
        "2012-01-01 12:00:00" | 0
        "2012-01-30 12:00:00" | 0
        "2012-02-01 12:00:00" | 0   // is equal to starting value has to check a chunk before
        "2012-02-01 12:00:01" | 1
        "2012-11-15 12:00:00" | 10
        "2012-12-01 12:00:00" | 10 // is equal to starting value has to check a chunk before
        "2012-12-01 12:00:01" | 11
        "2012-12-28 12:00:00" | 11
        "2012-12-30 12:00:00" | 11 // is outside of the generated range
    }

    @Unroll
    def "acceptIndexFile value=#queryValue indexFileFrom=#indexFileFrom indexFileTo=#indexFileTo"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.EQ, queryValue)
        def sdf = new SimpleDateFormat(DateTimeQueryValueRetriever.DATE_SEARCH_PATTERN)
        def indexFile = new NumberSnappyIndexFile<Long>(sdf.parse(indexFileFrom).getTime(), sdf.parse(indexFileTo).getTime(), Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryValue            | indexFileFrom         | indexFileTo           | accept
        "2012-10-01 12:00:00" | "2012-11-01 12:00:01" | "2013-10-01 11:59:59" | false
        "2012-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | true
        "2012-10-01 12:00:00" | "2012-10-01 12:00:01" | "2013-10-01 12:00:00" | false
        "2012-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 11:59:59" | true
        "2013-10-01 11:59:59" | "2012-10-01 12:00:00" | "2013-10-01 11:59:59" | true
        "2013-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 11:59:59" | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.EQ, "2013-10-01 11:59:59"))
        then:
        valueRetriever instanceof DateTimeQueryValueRetriever
    }
}