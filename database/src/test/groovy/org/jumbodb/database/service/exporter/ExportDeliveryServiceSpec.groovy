package org.jumbodb.database.service.exporter

import spock.lang.Specification

import java.util.concurrent.ExecutorService

/**
 * @author Carsten Hufe
 */
class ExportDeliveryServiceSpec extends Specification {
    def "startReplication should submit a new ExportDeliveryTask in executor service"() {
        setup:
        def executorServiceMock = Mock(ExecutorService)
        def service = new ExportDeliveryService()
        service.setExecutorService(executorServiceMock)
        def startReplication = new StartReplication()
        startReplication.setActivate(true)
        startReplication.setDeliveryChunkKey("chunk_key")
        startReplication.setVersion("my_version")
        startReplication.setHost("my_host")
        startReplication.setPort(13001)
        when:
        service.startReplication(startReplication)
        then:
        1 * executorServiceMock.submit(_ as ExportDeliveryTask)
        when:
        def repl = service.getReplications()
        then:
        repl.size() == 1
        repl.get(0).getCopyRateInBytes() == 0
        repl.get(0).getCopyRateInBytesUncompressed() == 0
        repl.get(0).getCurrentBytes() == 0
        repl.get(0).getDeliveryChunkKey() == "chunk_key"
        repl.get(0).getHost() == "my_host"
        repl.get(0).getStatus() == "Waiting for execution slot"
        repl.get(0).getState() == ExportDelivery.State.WAITING
    }

    def "deleteReplication should delete a finished replication"() {
        setup:
        def executorServiceMock = Mock(ExecutorService)
        def service = new ExportDeliveryService()
        service.setExecutorService(executorServiceMock)
        def startReplication = new StartReplication()
        startReplication.setActivate(true)
        startReplication.setDeliveryChunkKey("chunk_key")
        startReplication.setVersion("my_version")
        startReplication.setHost("my_host")
        startReplication.setPort(13001)
        service.startReplication(startReplication)
        when:
        service.deleteReplication(service.getReplications().get(0).getId())
        def repl = service.getReplications()
        then:
        repl.size() == 0
    }

    def "deleteReplication should stop a running replication"() {
        setup:
        def executorServiceMock = Mock(ExecutorService)
        def service = new ExportDeliveryService()
        service.setExecutorService(executorServiceMock)
        def startReplication = new StartReplication()
        startReplication.setActivate(true)
        startReplication.setDeliveryChunkKey("chunk_key")
        startReplication.setVersion("my_version")
        startReplication.setHost("my_host")
        startReplication.setPort(13001)
        service.startReplication(startReplication)
        def repl = service.getReplications().get(0)
        repl.setState(ExportDelivery.State.RUNNING)
        when:
        service.deleteReplication(repl.getId())
        def repls = service.getReplications()
        then:
        repls.size() == 0
        repl.getState() == ExportDelivery.State.ABORTED
    }

    def "stopReplication should abort a running replication"() {
        setup:
        def executorServiceMock = Mock(ExecutorService)
        def service = new ExportDeliveryService()
        service.setExecutorService(executorServiceMock)
        def startReplication = new StartReplication()
        startReplication.setActivate(true)
        startReplication.setDeliveryChunkKey("chunk_key")
        startReplication.setVersion("my_version")
        startReplication.setHost("my_host")
        startReplication.setPort(13001)
        service.startReplication(startReplication)
        def repl = service.getReplications().get(0)
        repl.setState(ExportDelivery.State.RUNNING)
        when:
        service.stopReplication(repl.getId())
        def repls = service.getReplications()
        then:
        repls.size() == 1
        repl.getState() == ExportDelivery.State.ABORTED
    }
}
