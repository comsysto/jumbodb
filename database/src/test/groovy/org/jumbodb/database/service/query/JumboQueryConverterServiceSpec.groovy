package org.jumbodb.database.service.query

import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
// CARSTEN implement tests
class JumboQueryConverterServiceSpec extends Specification {

    def "SQL conversion AND OR logic"() {

        //select * from test a where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h') and (g = 'g' or y = 'y' or o = 'o')) or x = 'x'

    }

    def "SQL conversion 1"() {
//        select * from test a where ((idx(aaaa, ddd) = 'aaa' AND bb = 'bb') OR user.cc = 'bb') limit 10
    }

    def "SQL conversion 2"() {
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where ((a = 'b' or z = 'z') and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'");
    }

    def "SQL conversion 3"() {
//        Select stmt = (Select) CCJSqlParserUtil.parse("select * from test a where (a = 'b' and z = 'z' and (c = 'd' or c = 'f' or g = 'h')) or x = 'x'");
    }

    def "SQL conversion 4"() {

    }


    def "verify supported SQL features"() {

    }
}
