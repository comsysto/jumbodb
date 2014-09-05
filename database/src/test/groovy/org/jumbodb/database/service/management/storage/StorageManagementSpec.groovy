package org.jumbodb.database.service.management.storage

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.RandomStringUtils
import org.jumbodb.common.query.ChecksumType
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.data.common.meta.ActiveProperties
import org.jumbodb.data.common.meta.CollectionProperties
import org.jumbodb.data.common.meta.DeliveryProperties
import org.jumbodb.data.common.meta.IndexProperties
import org.jumbodb.database.service.configuration.JumboConfiguration
import org.jumbodb.database.service.importer.ImportServer
import org.jumbodb.database.service.query.JumboSearcher
import org.jumbodb.database.service.query.data.DataStrategy
import org.jumbodb.database.service.query.index.IndexStrategy
import spock.lang.Specification
import spock.lang.Unroll

import java.text.SimpleDateFormat

/**
 * @author Carsten Hufe
 */
class StorageManagementSpec extends Specification {

    def "getMaintenanceTemporaryFilesInfo"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def importServerMock = Mock(ImportServer)
        def dataDir = Files.createTempDir()
        def dataContent = RandomStringUtils.randomAscii(1024 * 1024)
        FileUtils.write(new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery/part0001"), dataContent)
        def indexDir = Files.createTempDir()
        FileUtils.write(new File(indexDir.getAbsolutePath() + "/.tmp/test_delivery/part0001"), dataContent)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def info = storageManagement.getMaintenanceTemporaryFilesInfo()
        then:
        info.isImportRunning()
        info.getNumberOfAbortedDeliveries() == 1
        info.getTemporaryDataSize() == "2 MB"
        1 * importServerMock.isImportRunning() >> true
    }

    def "maintenanceCleanupTemporaryFiles"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def importServerMock = Mock(ImportServer)
        def dataDir = Files.createTempDir()
        def dataContent = RandomStringUtils.randomAscii(1024 * 1024)
        FileUtils.write(new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery/part0001"), dataContent)
        def indexDir = Files.createTempDir()
        FileUtils.write(new File(indexDir.getAbsolutePath() + "/.tmp/test_delivery/part0001"), dataContent)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        storageManagement.maintenanceCleanupTemporaryFiles()
        def info = storageManagement.getMaintenanceTemporaryFilesInfo()
        then:
        info.getNumberOfAbortedDeliveries() == 0
        info.getTemporaryDataSize() == "0 bytes"
        2 * importServerMock.isImportRunning() >> false
    }

    def "findQueryableCollections should return two queryable collections"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def querableCollections = storageManagement.findQueryableCollections()
        then:
        querableCollections.size() == 2
        querableCollections[0].getCollection() == "test_collection1"
        querableCollections[0].getDataStrategy() == "test_data_strategy"
        querableCollections[0].getSupportedOperations() as Set == [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT] as Set
        querableCollections[0].getIndexes().size() == 0
        then:
        querableCollections[1].getCollection() == "test_collection3"
        querableCollections[1].getDataStrategy() == "test_data_strategy"
        querableCollections[1].getSupportedOperations() as Set == [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT] as Set
        querableCollections[1].getIndexes().size() == 2
        querableCollections[1].getIndexes()[0].getName() == "test_index1"
        querableCollections[1].getIndexes()[0].getStrategy() == "test_index_strategy"
        querableCollections[1].getIndexes()[0].getSupportedOperations() as Set == [QueryOperation.BETWEEN, QueryOperation.EQ] as Set
        querableCollections[1].getIndexes()[1].getName() == "test_index2"
        querableCollections[1].getIndexes()[1].getStrategy() == "test_index_strategy"
        querableCollections[1].getIndexes()[1].getSupportedOperations() as Set == [QueryOperation.BETWEEN, QueryOperation.EQ] as Set
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "getLatestVersionInChunk should return the correct version"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def version = storageManagement.getLatestVersionInChunk("test_delivery1")
        then:
        version == "version2"
        when:
        version = storageManagement.getLatestVersionInChunk("test_delivery2")
        then:
        version == "version3"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "activateChunk and inactivateChunk should disable and enable an entire chunk"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        storageManagement.inactivateChunk("test_delivery3")
        then:
        def collections = storageManagement.findQueryableCollections()
        collections.every { it.collection != "test_collection3" } // this collection is only in test_delivery3 available }
        when:
        storageManagement.activateChunk("test_delivery3")
        collections = storageManagement.findQueryableCollections()
        then:
        collections.any { it.collection == "test_collection3" } // this collection is only in test_delivery3 available }
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "activateChunkedVersion should enable other versions"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        storageManagement.activateChunkedVersion("test_delivery1", "version1")
        def collections = storageManagement.findQueryableCollections()
        then:
        collections.any { it.collection == "test_collection2" }
        when:
        storageManagement.activateChunkedVersion("test_delivery1", "version2")
        collections = storageManagement.findQueryableCollections()
        then:
        collections.every { it.collection != "test_collection2" }
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "getChunkedDeliveryVersions should return correct collections"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def versions = storageManagement.getChunkedDeliveryVersions(false)
        then:
        versions.size() == 4
        versions[0].getChunkKey() == "test_delivery1"
        versions[0].getVersion() == "version2"
        versions[0].getInfo() == "some info"
        versions[0].collections[0].getChunkKey() == "test_delivery1"
        versions[0].collections[0].getCollectionName() == "test_collection1"
        versions[0].collections[0].getVersion() == "version2"
        versions[0].collections[0].getSourcePath() == "source path"
        versions[0].collections[0].getStrategy() == "test_data_strategy"

        versions[1].getChunkKey() == "test_delivery1"
        versions[1].getVersion() == "version1"
        versions[1].collections[0].getChunkKey() == "test_delivery1"
        versions[1].collections[0].getCollectionName() == "test_collection1"
        versions[1].collections[0].getVersion() == "version1"
        versions[1].collections[1].getChunkKey() == "test_delivery1"
        versions[1].collections[1].getCollectionName() == "test_collection2"
        versions[1].collections[1].getVersion() == "version1"

        versions[2].getChunkKey() == "test_delivery2"
        versions[2].getVersion() == "version3"
        versions[2].collections[0].getChunkKey() == "test_delivery2"
        versions[2].collections[0].getCollectionName() == "test_collection1"
        versions[2].collections[0].getVersion() == "version3"

        versions[3].getChunkKey() == "test_delivery3"
        versions[3].getVersion() == "version4"
        versions[3].collections[0].getChunkKey() == "test_delivery3"
        versions[3].collections[0].getCollectionName() == "test_collection3"
        versions[3].collections[0].getVersion() == "version4"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    @Unroll
    def "getActiveDeliveryVersion should return the active version in a collection collection=#collection delivery=#delivery == #expectedVersion"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        expect:
        storageManagement.getActiveDeliveryVersion(delivery) == expectedVersion
        cleanup:
        dataDir.delete()
        indexDir.delete()
        where:
        delivery         | expectedVersion
        "test_delivery1" | "version2"
        "test_delivery2" | "version3"
        "test_delivery3" | "version4"

    }

    def "getJumboCollections should return all jumbo collections"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def collections = storageManagement.getJumboCollections()
        then:
        collections.size() == 3
        // first collection
        collections[0].getName() == "test_collection1"
        collections[0].getUncompressedSize() == 0l
        collections[0].getChunks().size() == 2
        collections[0].getChunks()[0].getKey() == "test_delivery1"
        collections[0].getChunks()[0].getUncompressedSize() == 0l
        collections[0].getChunks()[0].getVersions().size() == 2
        collections[0].getChunks()[0].getVersions()[0].getVersion() == "version1"
        collections[0].getChunks()[0].getVersions()[0].getInfo() == "some info"
        collections[0].getChunks()[0].getVersions()[1].getVersion() == "version2"
        collections[0].getChunks()[0].getVersions()[1].getInfo() == "some info"
        collections[0].getChunks()[1].getKey() == "test_delivery2"
        collections[0].getChunks()[1].getVersions().size() == 1
        collections[0].getChunks()[1].getVersions()[0].getVersion() == "version3"
        collections[0].getChunks()[1].getVersions()[0].getInfo() == "some info"
        then:
        // second collection
        collections[1].getName() == "test_collection2"
        collections[1].getChunks().size() == 1
        collections[1].getChunks()[0].getKey() == "test_delivery1"
        collections[1].getChunks()[0].getVersions().size() == 1
        collections[1].getChunks()[0].getVersions()[0].getVersion() == "version1"
        collections[1].getChunks()[0].getVersions()[0].getInfo() == "some info"
        then:
        // third collection
        collections[2].getName() == "test_collection3"
        collections[2].getChunks().size() == 1
        collections[2].getChunks()[0].getKey() == "test_delivery3"
        collections[2].getChunks()[0].getVersions().size() == 1
        collections[2].getChunks()[0].getVersions()[0].getVersion() == "version4"
        collections[2].getChunks()[0].getVersions()[0].getInfo() == "some info"

        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "getIndexInfoForDelivery should map to IndexInfo and add sizes"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def indexInfos = storageManagement.getIndexInfoForDelivery("test_delivery3", "version4")
        then:
        indexInfos.size() == 6
        then:
        indexInfos[0].getCollection() == "test_collection3"
        indexInfos[0].getDeliveryKey() == "test_delivery3"
        indexInfos[0].getDeliveryVersion() == "version4"
        indexInfos[0].getIndexName() == "test_index1"
        indexInfos[0].getFileLength() == 155
        indexInfos[0].getFileName() == "index.properties"
        indexInfos[0].getChecksumType() == ChecksumType.NONE
        then:
        indexInfos[1].getCollection() == "test_collection3"
        indexInfos[1].getDeliveryKey() == "test_delivery3"
        indexInfos[1].getDeliveryVersion() == "version4"
        indexInfos[1].getIndexName() == "test_index1"
        indexInfos[1].getFileLength() == 11
        indexInfos[1].getFileName() == "part0001.idx"
        indexInfos[1].getChecksumType() == ChecksumType.NONE
             then:
        indexInfos[2].getCollection() == "test_collection3"
        indexInfos[2].getDeliveryKey() == "test_delivery3"
        indexInfos[2].getDeliveryVersion() == "version4"
        indexInfos[2].getIndexName() == "test_index1"
        indexInfos[2].getFileName() == "part0002.idx"
        then:
        indexInfos[3].getCollection() == "test_collection3"
        indexInfos[3].getDeliveryKey() == "test_delivery3"
        indexInfos[3].getDeliveryVersion() == "version4"
        indexInfos[3].getIndexName() == "test_index2"
        indexInfos[3].getFileLength() == 155
        indexInfos[3].getFileName() == "index.properties"
        indexInfos[3].getChecksumType() == ChecksumType.NONE
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "getDataInfoForDelivery should map to DataInfo and add sizes"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def dataInfos = storageManagement.getDataInfoForDelivery("test_delivery3", "version4")
        then:
        dataInfos.size() == 3
        dataInfos[0].getCollection() == "test_collection3"
        dataInfos[0].getDeliveryKey() == "test_delivery3"
        dataInfos[0].getDeliveryVersion() == "version4"
        dataInfos[0].getFileLength() == 143l
        dataInfos[0].getChecksumType() == ChecksumType.NONE
        when:
        def fileNames = (dataInfos.collect{ it.getFileName() } as Set)
        then:
        fileNames == ["collection.properties", "part0001", "part0002"] as Set
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "getInputStream(IndexInfo) should open a input stream to the given index file"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def indexInfos = storageManagement.getIndexInfoForDelivery("test_delivery3", "version4")
        def stream = storageManagement.getInputStream(indexInfos[1])
        then:
        IOUtils.toString(stream) == "Hello World"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "getInputStream(DataInfo) should open a input stream to the given data file"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        def dataInfos = storageManagement.getDataInfoForDelivery("test_delivery3", "version4")
        def stream = storageManagement.getInputStream(dataInfos[1])
        then:
        IOUtils.toString(stream) == "The real data"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "deleteChunkedVersion should delete all collections inside the delivery version"() {
        setup:
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyMock = Mock(DataStrategy)
        def importServerMock = Mock(ImportServer)
        dataStrategyMock.getStrategyName() >> "test_data_strategy"
        dataStrategyMock.getSupportedOperations() >> [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT]
        jumboSearcherMock.getDataStrategy(_, _) >> dataStrategyMock
        def indexStrategyMock = Mock(IndexStrategy)
        indexStrategyMock.getStrategyName() >> "test_index_strategy"
        indexStrategyMock.getSupportedOperations() >> [QueryOperation.BETWEEN, QueryOperation.EQ]
        jumboSearcherMock.getIndexStrategy(_, _, _) >> indexStrategyMock
        jumboSearcherMock.getIndexStrategy(_) >> indexStrategyMock

        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        createDefaultFileStructure(dataDir, indexDir)
        def storageManagement = new StorageManagement(new JumboConfiguration(12002, 12001, dataDir, indexDir), jumboSearcherMock, importServerMock)
        when:
        storageManagement.deleteChunkedVersion("test_delivery1", "version1")
        def collections = storageManagement.getJumboCollections()
        then: "deletes collection2 fully, version1 of collection1"
        collections.size() == 2
        collections[0].getName() == "test_collection1"
        collections[0].getChunks().size() == 2
        collections[0].getChunks()[0].getKey() == "test_delivery1"
        collections[0].getChunks()[0].getVersions().size() == 1
        collections[0].getChunks()[0].getVersions()[0].getVersion() == "version2"
        collections[1].getName() == "test_collection3"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def createDefaultFileStructure(File dataDir, File indexDir) {
        makeDelivery(dataDir, "test_delivery1", "version1", "2012-12-12 12:12:12")
        makeDataCollection(dataDir, "test_delivery1", "version1", "test_collection1", "2013-12-01")
        makeDataCollection(dataDir, "test_delivery1", "version1", "test_collection2", "2013-12-01")
        makeDelivery(dataDir, "test_delivery1", "version2", "2012-12-12 12:12:13")
        makeDataCollection(dataDir, "test_delivery1", "version2", "test_collection1", "2013-12-02")
        activateVersion(dataDir, "test_delivery1", "version2")

        makeDelivery(dataDir, "test_delivery2", "version3", "2012-12-12 12:12:12")
        makeDataCollection(dataDir, "test_delivery2", "version3", "test_collection1", "2013-12-03")
        activateVersion(dataDir, "test_delivery2", "version3")

        makeDelivery(dataDir, "test_delivery3", "version4", "2012-12-12 12:12:12")
        makeDataCollection(dataDir, "test_delivery3", "version4", "test_collection3", "2013-12-04")
        makeIndex(indexDir, "test_delivery3", "version4", "test_collection3", "test_index1")
        makeIndex(indexDir, "test_delivery3", "version4", "test_collection3", "test_index2")
        activateVersion(dataDir, "test_delivery3", "version4")
    }

    def activateVersion(dataDir, deliveryKey, version) {
        def propsFile = new File(dataDir.getAbsolutePath() + "/$deliveryKey/" + ActiveProperties.DEFAULT_FILENAME)
        Files.createParentDirs(propsFile)
        ActiveProperties.writeActiveFile(propsFile, version, true)
    }


    def makeDelivery(dataDir, deliveryKey, version, date) {
        def path = dataDir.getAbsolutePath() + "/$deliveryKey/$version/"
        def propsFile = new File(path + DeliveryProperties.DEFAULT_FILENAME)
        Files.createParentDirs(propsFile)
        def meta = new DeliveryProperties.DeliveryMeta(deliveryKey, version, date, "some info")
        DeliveryProperties.write(propsFile, meta)
    }

    def makeDataCollection(dataDir, deliveryKey, version, collection, date) {
        def path = dataDir.getAbsolutePath() + "/$deliveryKey/$version/$collection/"
        new File(path).mkdirs()
        def bytes = "The real data"
        new File(path + "part0001").text = bytes
        new File(path + "part0002").text = bytes
        def propsFile = new File(path + CollectionProperties.DEFAULT_FILENAME)
        def meta = new CollectionProperties.CollectionMeta(date, "source path", "test_data_strategy", "some info")
        CollectionProperties.write(propsFile, meta)
    }

    def makeIndex(indexDir, deliveryKey, version, collection, indexName) {
        def path = indexDir.getAbsolutePath() + "/$deliveryKey/$version/$collection/$indexName/"
        new File(path).mkdirs()
        def bytes = "Hello World"
        new File(path + "part0001.idx").text = bytes
        new File(path + "part0002.idx").text = bytes
        def propsFile = new File(path + IndexProperties.DEFAULT_FILENAME)
        Files.createParentDirs(propsFile)
        def sdf = new SimpleDateFormat("yyyy-MM-dd")
        def meta = new IndexProperties.IndexMeta(sdf.format(new Date()), indexName, "test_index_strategy", "source fields")
        IndexProperties.write(propsFile, meta)
    }
}
