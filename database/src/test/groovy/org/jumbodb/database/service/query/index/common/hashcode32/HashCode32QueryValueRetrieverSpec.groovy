package org.jumbodb.database.service.query.index.common.hashcode32

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.common.hashcode32.HashCode32QueryValueRetriever
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class HashCode32QueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify hashcode parsing #queryValue"() {
        expect:
        def retriever = new HashCode32QueryValueRetriever(new IndexQuery("testIndex", QueryOperation.EQ, queryValue))
        retriever.getValue() == converted
        where:
        queryValue            | converted
        "HelloWorld"          | "HelloWorld".hashCode()
        Arrays.asList(12, 34) | Arrays.asList(12, 34).hashCode()
        new Long(45)          | new Long(45).hashCode()
    }
}
