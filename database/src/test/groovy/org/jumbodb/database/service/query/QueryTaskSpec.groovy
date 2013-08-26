package org.jumbodb.database.service.query

import org.codehaus.jackson.map.ObjectMapper
import org.jumbodb.common.query.JumboQuery
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class QueryTaskSpec extends Specification {
    def "querying"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def objectMapperMock = Mock(ObjectMapper)
        def resultWriterMock = Mock(DatabaseQuerySession.ResultWriter)
        def databaseQuerySessionMock = Mock(DatabaseQuerySession)
        def jumboQuery = new JumboQuery()
        jumboQuery.setLimit(3)
        def queryTask = new QueryTaskWithSessionMock(socketMock, 5, jumboSearcherMock, objectMapperMock, databaseQuerySessionMock)
        when:
        queryTask.run()
        then:
        1 * databaseQuerySessionMock.query(_) >> { queryHandler ->
            queryHandler[0].onQuery("mycollection", "jsonquery".getBytes("UTF-8"), resultWriterMock)
        }
        1 * jumboSearcherMock.findResultAndWriteIntoCallback("mycollection", _, _) >> { collection, query, resultWriter ->
            assert resultWriter.needsMore(jumboQuery) == true
            resultWriter.writeResult("Result 1".getBytes("UTF-8"))
            assert resultWriter.needsMore(jumboQuery) == true
            resultWriter.writeResult("Result 2".getBytes("UTF-8"))
            assert resultWriter.needsMore(jumboQuery) == true
            resultWriter.writeResult("Result 3".getBytes("UTF-8"))
            assert resultWriter.needsMore(jumboQuery) == false

        }
        1 * objectMapperMock.readValue(_, _) >> jumboQuery

    }

    class QueryTaskWithSessionMock extends QueryTask {
        private DatabaseQuerySession session

        QueryTaskWithSessionMock(Socket s, int clientID, JumboSearcher jumboSearcher, ObjectMapper jsonMapper, DatabaseQuerySession session) {
            super(s, clientID, jumboSearcher, jsonMapper)
            this.session = session
        }

        @Override
        protected DatabaseQuerySession createDatabaseQuerySession() throws IOException {
            return session
        }
    }
}
