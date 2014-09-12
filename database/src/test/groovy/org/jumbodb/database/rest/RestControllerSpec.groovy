package org.jumbodb.database.rest

import org.jumbodb.database.service.exporter.ExportDeliveryService
import org.jumbodb.database.service.exporter.StartReplication
import org.jumbodb.database.service.management.storage.StorageManagement
import org.jumbodb.database.service.management.storage.dto.maintenance.TemporaryFiles
import org.jumbodb.database.service.queryutil.QueryUtilService
import org.jumbodb.database.service.queryutil.dto.QueryResult
import spock.lang.Specification


/**
 * @author Carsten Hufe
 */
class RestControllerSpec extends Specification {
    def "maintenanceInfo"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        def returnValueMock = Mock(TemporaryFiles)
        when:
        controller.maintenanceInfo() == returnValueMock
        then:
        1 * storageManagementMock.getMaintenanceTemporaryFilesInfo() >> returnValueMock
    }

    def "maintenanceCleanup"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.maintenanceCleanup()
        then:
        1 * storageManagementMock.maintenanceCleanupTemporaryFiles()
    }

    def "triggerReloadDatabases"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.triggerReloadDatabases()
        then:
        1 * storageManagementMock.triggerReloadDatabases()
    }

    def "getJumboCollections"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        def returnValueMock = Mock(List)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.getJumboCollections() == returnValueMock
        then:
        1 * storageManagementMock.getJumboCollections() >> returnValueMock
    }

    def "getChunkedDeliveryVersions"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        def returnValueMock = Mock(List)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.getChunkedDeliveryVersions() == returnValueMock
        then:
        1 * storageManagementMock.getChunkedDeliveryVersions() >> returnValueMock
    }

    def "getQueryableCollections"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        def returnValueMock = Mock(List)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.getQueryableCollections() == returnValueMock
        then:
        1 * storageManagementMock.findQueryableCollections() >> returnValueMock
    }

    def "query"() {
        setup:
        def controller = new RestController()
        def queryUtilServiceMock = Mock(QueryUtilService)
        def returnValueMock = Mock(QueryResult)
        controller.setQueryUtilService(queryUtilServiceMock)
        when:
        controller.query("collection", "my_query") == returnValueMock
        then:
        1 * queryUtilServiceMock.findDocumentsByQuery("collection", "my_query", -1) >> returnValueMock
    }


    def "activateChunk"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.activateChunk("chunk_key").type == "activate"
        then:
        1 * storageManagementMock.activateChunk("chunk_key")
    }

    def "inactivateChunk"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.inactivateChunk("chunk_key").type == "activate"
        then:
        1 * storageManagementMock.inactivateChunk("chunk_key")
    }

    def "activateChunkedVersion"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.activateChunkedVersion("chunk_key", "version").type == "activate"
        then:
        1 * storageManagementMock.activateChunkedVersion("chunk_key", "version")
    }

    def "deleteChunkedVersion"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.deleteChunkedVersion("chunk_key", "version").type == "delete"
        then:
        1 * storageManagementMock.deleteChunkedVersion("chunk_key", "version")
    }

    def "startReplication"() {
        setup:
        def controller = new RestController()
        def exportDeliveryServiceMock = Mock(ExportDeliveryService)
        def replicationMock = Mock(StartReplication)
        controller.setExportDeliveryService(exportDeliveryServiceMock)
        when:
        controller.startReplication(replicationMock).type == "success"
        then:
        1 * exportDeliveryServiceMock.startReplication(replicationMock)
    }

    def "getReplications"() {
        setup:
        def controller = new RestController()
        def exportDeliveryServiceMock = Mock(ExportDeliveryService)
        def returnMock = Mock(List)
        controller.setExportDeliveryService(exportDeliveryServiceMock)
        when:
        controller.getReplications() == returnMock
        then:
        1 * exportDeliveryServiceMock.getReplications() >> returnMock
    }

    def "abortReplications"() {
        setup:
        def controller = new RestController()
        def exportDeliveryServiceMock = Mock(ExportDeliveryService)
        controller.setExportDeliveryService(exportDeliveryServiceMock)
        when:
        controller.abortReplications("test_id").type = "abort"
        then:
        1 * exportDeliveryServiceMock.stopReplication("test_id")
    }

    def "deleteReplications"() {
        setup:
        def controller = new RestController()
        def exportDeliveryServiceMock = Mock(ExportDeliveryService)
        controller.setExportDeliveryService(exportDeliveryServiceMock)
        when:
        controller.deleteReplications("test_id").type = "delete"
        then:
        1 * exportDeliveryServiceMock.deleteReplication("test_id")
    }
}
