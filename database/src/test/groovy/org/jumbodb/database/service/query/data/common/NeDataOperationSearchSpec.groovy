package org.jumbodb.database.service.query.data.common

import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class NeDataOperationSearchSpec extends Specification {
    def operation = new NeDataOperationSearch()

    @Unroll
    def "matches non equality #value ne #testValue == #isNotEquals"() {
        expect:
        operation.matches(testValue, value) == isNotEquals
        where:
        value     | testValue   | isNotEquals
        "testStr" | "testStr"   | false
        "testStr" | "testStrNr" | true
        1         | 1           | false
        1         | 2           | true
    }
}
