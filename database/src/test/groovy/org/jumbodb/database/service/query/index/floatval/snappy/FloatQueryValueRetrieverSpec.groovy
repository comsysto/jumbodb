package org.jumbodb.database.service.query.index.floatval.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class FloatQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify double parsing #queryValue"() {
        expect:
        def retriever = new FloatQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        1234f      | 1234f
        1.123f     | 1.123f
    }

    def "expect exception on bullshit string"() {
        when:
        new FloatQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, "bullshit"))
        then:
        thrown ClassCastException
    }
}