package org.jumbodb.database.service.query.index.floatval.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class FloatBetweenQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify float parsing #queryValue"() {
        expect:
        def retriever = new FloatBetweenQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, queryValue))
        retriever.getValue() == converted
        where:
        queryValue     | converted
        [10f, 333.33f] | [10f, 333.33f]
        [11.11f, 44f]  | [11.11f, 44f]
    }

    def "expect exception on bullshit string"() {
        when:
        new FloatBetweenQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}