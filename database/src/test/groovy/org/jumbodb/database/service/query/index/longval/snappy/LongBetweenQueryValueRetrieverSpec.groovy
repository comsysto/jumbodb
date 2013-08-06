package org.jumbodb.database.service.query.index.longval.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.doubleval.snappy.DoubleBetweenQueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongBetweenQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify long parsing #queryValue"() {
        expect:
        def retriever = new LongBetweenQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        [10l, 333] | [10l, 333l]
        [11l, 44l] | [11l, 44l]
    }

    def "expect exception on bullshit string"() {
        when:
        new LongBetweenQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}
