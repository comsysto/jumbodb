package org.jumbodb.database.service.query.index.doubleval.snappy

import org.apache.commons.lang.UnhandledException
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeBetweenQueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class DoubleBetweenQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify double parsing #queryValue"() {
        expect:
        def retriever = new DoubleBetweenQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, queryValue))
        retriever.getValue() == converted
        where:
        queryValue     | converted
        [10d, 333.33d] | [10d, 333.33d]
        [11.11d, 44d]  | [11.11d, 44d]
    }

    def "expect exception on bullshit string"() {
        when:
        new DoubleBetweenQueryValueRetriever(new QueryClause(QueryOperation.BETWEEN, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}
