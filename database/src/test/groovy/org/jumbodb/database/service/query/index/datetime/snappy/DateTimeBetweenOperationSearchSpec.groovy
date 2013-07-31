package org.jumbodb.database.service.query.index.datetime.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import spock.lang.Unroll

import java.text.SimpleDateFormat

/**
 * @author Carsten Hufe
 */
class DateTimeBetweenOperationSearchSpec extends spock.lang.Specification {
    def operation = new DateTimeBetweenOperationSearch()

    @Unroll
    def "between match #from < #testValue > #to == #isBetween"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(from, to))
        def sdf = new SimpleDateFormat(DateTimeQueryValueRetriever.DATE_SEARCH_PATTERN)

        operation.matching(sdf.parse(testValue).getTime(), operation.getQueryValueRetriever(queryClause)) == isBetween
        where:
        from                  | to                    | testValue             | isBetween
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2012-11-01 12:00:00" | true
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2012-09-30 12:00:00" | false
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2012-10-01 12:00:00" | false
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2013-10-01 12:00:00" | false
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2013-10-01 11:59:59" | true
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2012-10-01 12:00:01" | true
    }

    def "findFirstMatchingChunk"() {
//        when:
//        operation.findFirstMatchingChunk()
//        then:

    }

    @Unroll
    def "acceptIndexFile from=#indexFileFrom to=#indexFileTo "() {
        expect:
        def queryClause = new QueryClause(QueryOperation.BETWEEN, Arrays.asList(queryFrom, queryTo))
        def sdf = new SimpleDateFormat(DateTimeQueryValueRetriever.DATE_SEARCH_PATTERN)
        def indexFile = new NumberSnappyIndexFile<Long>(sdf.parse(indexFileFrom).getTime(), sdf.parse(indexFileTo).getTime(), Mock(File));
        operation.acceptIndexFile(operation.getQueryValueRetriever(queryClause), indexFile) == accept
        where:
        queryFrom             | queryTo               | indexFileFrom         | indexFileTo           | accept
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2012-11-01 12:00:01" | "2013-10-01 11:59:59" | true
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | false
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2012-10-01 12:00:01" | "2013-10-01 12:00:00" | true
        "2012-10-01 12:00:00" | "2013-10-01 12:00:00" | "2012-10-01 12:00:00" | "2013-10-01 11:59:59" | false
    }

    def "getQueryValueRetriever"() {
        when:
        def valueRetriever = operation.getQueryValueRetriever(new QueryClause(QueryOperation.EQ, []))
        then:
        valueRetriever instanceof DateTimeBetweenQueryValueRetriever
    }
}