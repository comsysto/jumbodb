package org.jumbodb.database.service.query.index.common.longval

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.longval.LongQueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class LongQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify long parsing #queryValue"() {
        expect:
        def retriever = new LongQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue | converted
        1234l      | 1234l
        -1234l     | -1234l
    }

    def "expect exception on bullshit string"() {
        when:
        new LongQueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, "bullshit"))
        then:
        thrown ClassCastException
    }
}