package org.jumbodb.database.service.query.index.doubleval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DoubleQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify double parsing #queryValue"() {
        expect:
        def retriever = new DoubleQueryValueRetriever(new QueryClause(QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        1234d      | 1234d
        1.123d     | 1.123d
    }

    def "expect exception on bullshit string"() {
        when:
        new DoubleQueryValueRetriever(new QueryClause(QueryOperation.EQ, "bullshit"))
        then:
        thrown ClassCastException
    }
}
