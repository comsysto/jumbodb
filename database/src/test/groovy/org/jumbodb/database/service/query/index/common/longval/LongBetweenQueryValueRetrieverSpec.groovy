package org.jumbodb.database.service.query.index.common.longval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.longval.LongBetweenQueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongBetweenQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify long parsing #queryValue"() {
        expect:
        def retriever = new LongBetweenQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        [10l, 333] | [10l, 333l]
        [11l, 44l] | [11l, 44l]
    }

    def "expect exception on bullshit string"() {
        when:
        new LongBetweenQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}
