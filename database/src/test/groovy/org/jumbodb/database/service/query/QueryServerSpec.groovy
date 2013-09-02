package org.jumbodb.database.service.query

import org.jumbodb.database.service.configuration.JumboConfiguration
import spock.lang.Specification

import java.util.concurrent.ExecutorService

/**
 * @author Carsten Hufe
 */
class QueryServerSpec extends Specification {

    def "test start up and connect"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataFileMock = Mock(File)
        def indexFileMock = Mock(File)
        def config = new JumboConfiguration(12002, 12001, dataFileMock, indexFileMock)
        def queryServer = new QueryServer(config, jumboSearcherMock, 500l)
        def executorMock = Mock(ExecutorService)
        queryServer.setServerSocketExecutor(executorMock)
        queryServer.start()
        when:
        def socket = new Socket("localhost", 12002)
        Thread.sleep(1000)
        then:
        queryServer.isServerActive()
        socket.isConnected()
        1 * executorMock.submit(_ as QueryTask)
        cleanup:
        queryServer.stop()
        socket.close()
    }

    def "test start up and stop"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataFileMock = Mock(File)
        def indexFileMock = Mock(File)
        def config = new JumboConfiguration(13002, 13001, dataFileMock, indexFileMock)
        def queryServer = new QueryServer(config, jumboSearcherMock, 500l)
        queryServer.start()
        when:
        queryServer.stop()
        new Socket("localhost", 13002)
        then:
        thrown ConnectException
        !queryServer.isServerActive()
    }
}
