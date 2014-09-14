package org.jumbodb.database.service.query.index.integer.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class IntegerBetweenQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify long parsing #queryValue"() {
        expect:
        def retriever = new IntegerBetweenQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        [10, 333]  | [10, 333]
        [11, 44]   | [11, 44]
        [11l, 44l] | [11, 44]
    }

    def "expect exception on bullshit string"() {
        when:
        new IntegerBetweenQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.BETWEEN, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}
