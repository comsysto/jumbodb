package org.jumbodb.database.service.query.index.longval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleQueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify long parsing #queryValue"() {
        expect:
        def retriever = new LongQueryValueRetriever(new QueryClause(QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        1234l      | 1234l
        -1234l     | -1234l
    }

    def "expect exception on bullshit string"() {
        when:
        new LongQueryValueRetriever(new QueryClause(QueryOperation.EQ, "bullshit"))
        then:
        thrown ClassCastException
    }
}