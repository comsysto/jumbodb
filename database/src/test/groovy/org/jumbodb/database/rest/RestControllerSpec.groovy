package org.jumbodb.database.rest

import org.jumbodb.database.service.exporter.ExportDeliveryService
import org.jumbodb.database.service.exporter.StartReplication
import org.jumbodb.database.service.management.status.StatusService
import org.jumbodb.database.service.management.status.dto.ServerInformation
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

    def "activateChunkedVersionForAllCollections"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.activateChunkedVersionForAllCollections("chunk_key", "version").type == "activate"
        then:
        1 * storageManagementMock.activateChunkedVersionForAllCollections("chunk_key", "version")
    }

    def "activateChunkedVersionInCollection"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.activateChunkedVersionInCollection("chunk_key", "version", "collection").type == "activate"
        then:
        1 * storageManagementMock.activateChunkedVersionInCollection("chunk_key", "version", "collection")
    }

    def "deleteChunkedVersionForAllCollections"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.deleteChunkedVersionForAllCollections("chunk_key", "version").type == "delete"
        then:
        1 * storageManagementMock.deleteChunkedVersionForAllCollections("chunk_key", "version")
    }

    def "deleteChunkedVersionInCollection"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.deleteChunkedVersionInCollection("chunk_key", "version", "collection").type == "delete"
        then:
        1 * storageManagementMock.deleteChunkedVersionInCollection("chunk_key", "version", "collection")
    }

    def "deleteCompleteCollection"() {
        setup:
        def controller = new RestController()
        def storageManagementMock = Mock(StorageManagement)
        controller.setStorageManagement(storageManagementMock)
        when:
        controller.deleteChunkedVersionInCollection("chunk_key", "version", "collection").type == "delete"
        then:
        1 * storageManagementMock.deleteChunkedVersionInCollection("chunk_key", "version", "collection")
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
