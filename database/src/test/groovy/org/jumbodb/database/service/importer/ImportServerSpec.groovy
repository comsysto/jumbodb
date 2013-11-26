package org.jumbodb.database.service.importer

import org.jumbodb.database.service.configuration.JumboConfiguration
import org.jumbodb.database.service.query.JumboSearcher
import org.jumbodb.database.service.query.QueryServer
import org.jumbodb.database.service.query.QueryTask
import org.jumbodb.database.service.query.data.DataStrategyManager
import org.jumbodb.database.service.query.index.IndexStrategyManager
import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadPoolExecutor

/**
 * @author Carsten Hufe
 */
class ImportServerSpec extends Specification {

    def "test start up and connect"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def config = new JumboConfiguration(12002, 12001, Mock(File), Mock(File))
        def importServer = new ImportServer(config, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock)
        def executorMock = Mock(ThreadPoolExecutor)
        importServer.setExecutorService(executorMock)
        importServer.start()
        when:
        def socket = new Socket("localhost", 12001)
        Thread.sleep(1000)
        then:
        importServer.isServerActive()
        socket.isConnected()
        1 * executorMock.submit(_ as ImportTask)
        cleanup:
        importServer.stop()
        socket.close()
    }

    def "test start up and stop"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def config = new JumboConfiguration(13002, 13001, Mock(File), Mock(File))
        def importServer = new ImportServer(config, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock)
        def executorMock = Mock(ThreadPoolExecutor)
        importServer.setExecutorService(executorMock)
        importServer.start()
        when:
        importServer.stop()
        new Socket("localhost", 13002)
        then:
        thrown ConnectException
        !importServer.isServerActive()
    }
}
