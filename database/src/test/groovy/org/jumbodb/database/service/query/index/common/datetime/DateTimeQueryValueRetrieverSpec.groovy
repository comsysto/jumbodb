package org.jumbodb.database.service.query.index.common.datetime

import org.apache.commons.lang.UnhandledException
import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.datetime.DateTimeQueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DateTimeQueryValueRetrieverSpec extends Specification {

    @Unroll
    def "verify date string parsing #queryValue"() {
        expect:
        def retriever = new DateTimeQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue            | converted
        "2012-12-12 13:01:00" | 1355313660000
        "2012-12-13 15:01:00" | 1355407260000
    }

    @Unroll
    def "verify date long parsing #queryValue"() {
        expect:
        def retriever = new DateTimeQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue    | converted
        1355313660000 | 1355313660000
        1355407260000 | 1355407260000
    }

    def "expect exception on bullshit string"() {
        when:
        new DateTimeQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, "bullshit"))
        then:
        thrown UnhandledException
    }
}