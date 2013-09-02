package org.jumbodb.database.service.query

import org.codehaus.jackson.map.ObjectMapper
import org.jumbodb.common.query.JumboQuery
import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

/**
 * @author Carsten Hufe
 */
class QueryTaskSpec extends Specification {
    def "querying"() {
        setup:
        def executorServiceMock = Mock(ExecutorService)
        def futureMock = Mock(Future)
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
        def queryTask = new QueryTaskWithSessionMock(socketMock, 5, jumboSearcherMock, objectMapperMock, databaseQuerySessionMock, executorServiceMock)
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
        1 * executorServiceMock.submit(_) >> { val ->
            val[0].call()
            return futureMock
        }
        1 * futureMock.get(_, _) >> 1

    }

    class QueryTaskWithSessionMock extends QueryTask {
        private DatabaseQuerySession session

        QueryTaskWithSessionMock(Socket s, int clientID, JumboSearcher jumboSearcher, ObjectMapper jsonMapper, DatabaseQuerySession session, ExecutorService executorService) {
            super(s, clientID, jumboSearcher, jsonMapper, executorService, 100l)
            this.session = session
        }

        @Override
        protected DatabaseQuerySession createDatabaseQuerySession() throws IOException {
            return session
        }
    }
}
