/**
 * @author Carsten Hufe
 */
class HelloSpock extends spock.lang.Specification {
    def "length of Spock's and his friends' names"() {
        expect:
        name.size() == length

        where:
        name     | length
        "Spock"  | 5
        "Kirk"   | 4
        "Scotty" | 6
    }


    def "length of Spock's and his friends' names2"() {
        expect:
        println("expect--")
        name.size() == length
        when:
        def ns = name.size()
        println("when--")
        then:
        println("then--")
        ns == length



        where:
        name     | length
        "Spock"  | 5
        "Kirk"   | 4
        "Scotty" | 6
    }
}