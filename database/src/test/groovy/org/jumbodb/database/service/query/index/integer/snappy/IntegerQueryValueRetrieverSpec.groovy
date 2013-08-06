package org.jumbodb.database.service.query.index.integer.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class IntegerQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify integer parsing #queryValue"() {
        expect:
        def retriever = new IntegerQueryValueRetriever(new QueryClause(QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        1234       | 1234
        -1234      | -1234
    }

    def "expect exception on bullshit string"() {
        when:
        new IntegerQueryValueRetriever(new QueryClause(QueryOperation.EQ, "bullshit"))
        then:
        thrown ClassCastException
    }
}
