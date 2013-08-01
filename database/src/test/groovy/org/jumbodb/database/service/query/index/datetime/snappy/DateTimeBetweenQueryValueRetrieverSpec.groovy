package org.jumbodb.database.service.query.index.datetime.snappy

import org.apache.commons.lang.UnhandledException
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DateTimeBetweenQueryValueRetrieverSpec extends spock.lang.Specification {

    @Unroll
    def "verify date string parsing #queryValue"() {
        expect:
        def retriever = new DateTimeBetweenQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        ["2012-12-12 13:01:00", "2012-12-13 14:01:00"] | [1355313660000, 1355403660000]
        ["2012-12-13 15:01:00", "2012-12-17 14:01:00"] | [1355407260000, 1355749260000]
    }

    @Unroll
    def "verify date long parsing #queryValue"() {
        expect:
        def retriever = new DateTimeBetweenQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        [1355313660000, 1355403660000] | [1355313660000, 1355403660000]
        [1355407260000, 1355749260000] | [1355407260000, 1355749260000]
    }

    def "expect exception on bullshit string"() {
        when:
        new DateTimeBetweenQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, ["bullshit", "andso"]))
        then:
        thrown UnhandledException
    }
}
