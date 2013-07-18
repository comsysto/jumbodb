package org.jumbodb.benchmark.generator

import spock.lang.Specification
import spock.lang.Unroll

class DataCreatorSpec extends Specification {


    @Unroll("create random json document with size of #docSizeInBytes bytes")
    def "create random json documents of given size"(){
        setup:
        def dataCreator = new DataCreator();
        expect:
        docSizeInBytes == dataCreator.createJSONDocument(docSizeInBytes).length()
        where:
        docSizeInBytes << [20, 50, 100, 500, 1000]
    }


}