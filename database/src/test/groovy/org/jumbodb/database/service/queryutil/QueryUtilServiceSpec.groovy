package org.jumbodb.database.service.queryutil

import org.jumbodb.database.service.query.JumboSearcher
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
// CARSTEN test sql
class QueryUtilServiceSpec extends Specification {

    def "findDocumentsByJsonQuery"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def queryUtilService = new QueryUtilService()
        queryUtilService.setJumboSearcher(jumboSearcherMock)
        def query = """
        { "indexQuery": [], "jsonQuery": [], "limit": 10}
        """
        when:
        def queryResult = queryUtilService.findDocumentsByJsonQuery(query, 20)
        then:
        queryResult.getMessage() == null
        queryResult.getResults()[0] == [sample: "result", anumber: 4]
        queryResult.getResults()[1] == [sample: "another result", anumber: 6]
        1 * jumboSearcherMock.findResultAndWriteIntoCallback(_, _) >> { jumboQuery, resultWriter ->
            resultWriter.writeResult("""{"sample": "result", "anumber": 4}""".getBytes("UTF-8"))
            resultWriter.writeResult("""{"sample": "another result", "anumber": 6}""".getBytes("UTF-8"))
            return 2
        }
    }

    def "on RuntimeException"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def queryUtilService = new QueryUtilService()
        queryUtilService.setJumboSearcher(jumboSearcherMock)
        def query = """
        { "indexQuery": [], "jsonQuery": [], "limit": 10}
        """
        when:
        def queryResult = queryUtilService.findDocumentsByJsonQuery(query, 20)
        then:
        queryResult.getResults() == null
        queryResult.getMessage() == "test exception"
        1 * jumboSearcherMock.findResultAndWriteIntoCallback(_, _) >> { jumboQuery, resultWriter ->
            throw new IOException("test exception")
        }
    }
}
