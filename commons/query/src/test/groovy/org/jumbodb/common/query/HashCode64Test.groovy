package org.jumbodb.common.query

import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class HashCode64Test extends Specification {
    @Unroll
    def "test hash #string == hash(#expectedHash)"() {
        expect:
        HashCode64.hash(string) == expectedHash
        where:
        string          | expectedHash
        "Hello World 1" | -6835400636449306160l
        "Hello World 2" | -6835400636449306159l
        "What ever"     | -4289189462688251243l
    }
}
