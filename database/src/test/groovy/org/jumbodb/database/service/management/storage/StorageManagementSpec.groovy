package org.jumbodb.database.service.management.storage

import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.lang.RandomStringUtils
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.data.common.meta.ActiveProperties
import org.jumbodb.data.common.meta.CollectionProperties
import org.jumbodb.data.common.meta.IndexProperties
import org.jumbodb.data.common.snappy.SnappyChunksUtil
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
        querableCollections.size() == 3
        querableCollections[0].getCollection() == "test_collection1"
        querableCollections[0].getDataStrategy() == "test_data_strategy"
        querableCollections[0].getSupportedOperations() as Set == [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT] as Set
        querableCollections[0].getIndexes().size() == 0
        then:
        querableCollections[1].getCollection() == "test_collection2"
        querableCollections[1].getDataStrategy() == "test_data_strategy"
        querableCollections[1].getSupportedOperations() as Set == [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT] as Set
        querableCollections[1].getIndexes().size() == 0
        then:
        querableCollections[2].getCollection() == "test_collection3"
        querableCollections[2].getDataStrategy() == "test_data_strategy"
        querableCollections[2].getSupportedOperations() as Set == [QueryOperation.EQ, QueryOperation.GT, QueryOperation.LT] as Set
        querableCollections[2].getIndexes().size() == 2
        querableCollections[2].getIndexes()[0].getName() == "test_index1"
        querableCollections[2].getIndexes()[0].getStrategy() == "test_index_strategy"
        querableCollections[2].getIndexes()[0].getSupportedOperations() as Set == [QueryOperation.BETWEEN, QueryOperation.EQ] as Set
        querableCollections[2].getIndexes()[1].getName() == "test_index2"
        querableCollections[2].getIndexes()[1].getStrategy() == "test_index_strategy"
        querableCollections[2].getIndexes()[1].getSupportedOperations() as Set == [QueryOperation.BETWEEN, QueryOperation.EQ] as Set
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
        storageManagement.getActiveDeliveryVersion(collection, delivery) == expectedVersion
        cleanup:
        dataDir.delete()
        indexDir.delete()
        where:
        collection         | delivery         | expectedVersion
        "test_collection1" | "test_delivery1" | "version2"
        "test_collection1" | "test_delivery2" | "version3"
        "test_collection2" | "test_delivery1" | "version1"
        "test_collection3" | "test_delivery3" | "version4"

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
        collections[0].getUncompressedSize() == 78l
        collections[0].getChunks().size() == 2
        collections[0].getChunks()[0].getKey() == "test_delivery1"
        collections[0].getChunks()[0].getUncompressedSize() == 52l
        collections[0].getChunks()[0].getVersions().size() == 2
        collections[0].getChunks()[0].getVersions()[0].getVersion() == "version2"
        collections[0].getChunks()[0].getVersions()[0].getInfo() == "some info"
        collections[0].getChunks()[0].getVersions()[0].getUncompressedSize() == 26l
        collections[0].getChunks()[0].getVersions()[1].getVersion() == "version1"
        collections[0].getChunks()[0].getVersions()[1].getInfo() == "some info"
        collections[0].getChunks()[0].getVersions()[1].getUncompressedSize() == 26l
        collections[0].getChunks()[1].getKey() == "test_delivery2"
        collections[0].getChunks()[1].getVersions().size() == 1
        collections[0].getChunks()[1].getVersions()[0].getVersion() == "version3"
        collections[0].getChunks()[1].getVersions()[0].getInfo() == "some info"
        collections[0].getChunks()[1].getVersions()[0].getUncompressedSize() == 26l

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

    def "getMetaIndexForDelivery should return the delivery meta data"() {
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
        def metaIndex = storageManagement.getMetaIndexForDelivery("test_delivery1", "version1")
        then:
        metaIndex.size() == 0
        when:
        metaIndex = storageManagement.getMetaIndexForDelivery("test_delivery3", "version4")
        then:
        metaIndex.size() == 2
        metaIndex[0].getCollection() == "test_collection3"
        metaIndex[0].getDeliveryKey() == "test_delivery3"
        metaIndex[0].getDeliveryVersion() == "version4"
        metaIndex[0].getIndexName() == "test_index1"
        metaIndex[0].getIndexSourceFields() == "source fields"
        metaIndex[0].getIndexStrategy() == "test_index_strategy"
        metaIndex[1].getCollection() == "test_collection3"
        metaIndex[1].getDeliveryKey() == "test_delivery3"
        metaIndex[1].getDeliveryVersion() == "version4"
        metaIndex[1].getIndexName() == "test_index2"
        metaIndex[1].getIndexSourceFields() == "source fields"
        metaIndex[1].getIndexStrategy() == "test_index_strategy"
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
        def metaIndex = storageManagement.getMetaIndexForDelivery("test_delivery3", "version4")
        def indexInfos = storageManagement.getIndexInfoForDelivery(metaIndex)
        then:
        indexInfos.size() == 4
        indexInfos[0].getCollection() == "test_collection3"
        indexInfos[0].getDeliveryKey() == "test_delivery3"
        indexInfos[0].getDeliveryVersion() == "version4"
        indexInfos[0].getIndexName() == "test_index1"
        indexInfos[0].getFileLength() == 11l
        indexInfos[0].getFilename() == "part0001.odx"
        indexInfos[0].getIndexStrategy() == "test_index_strategy"
        then:
        indexInfos[1].getCollection() == "test_collection3"
        indexInfos[1].getDeliveryKey() == "test_delivery3"
        indexInfos[1].getDeliveryVersion() == "version4"
        indexInfos[1].getIndexName() == "test_index1"
        indexInfos[1].getFileLength() == 11l
        indexInfos[1].getFilename() == "part0002.odx"
        indexInfos[1].getIndexStrategy() == "test_index_strategy"
        then:
        indexInfos[2].getCollection() == "test_collection3"
        indexInfos[2].getDeliveryKey() == "test_delivery3"
        indexInfos[2].getDeliveryVersion() == "version4"
        indexInfos[2].getIndexName() == "test_index2"
        indexInfos[2].getFileLength() == 11l
        indexInfos[2].getFilename() == "part0001.odx"
        indexInfos[2].getIndexStrategy() == "test_index_strategy"
        then:
        indexInfos[3].getCollection() == "test_collection3"
        indexInfos[3].getDeliveryKey() == "test_delivery3"
        indexInfos[3].getDeliveryVersion() == "version4"
        indexInfos[3].getIndexName() == "test_index2"
        indexInfos[3].getFileLength() == 11l
        indexInfos[3].getFilename() == "part0002.odx"
        indexInfos[3].getIndexStrategy() == "test_index_strategy"
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
        def metaIndex = storageManagement.getMetaDataForDelivery("test_delivery3", "version4", true)
        def dataInfos = storageManagement.getDataInfoForDelivery(metaIndex)
        then:
        dataInfos.size() == 2
        dataInfos[0].getCollection() == "test_collection3"
        dataInfos[0].getDeliveryKey() == "test_delivery3"
        dataInfos[0].getDeliveryVersion() == "version4"
        dataInfos[0].getFileLength() == 13l
        dataInfos[0].getDataStrategy() == "test_data_strategy"
        when:
        def fileNames = (dataInfos.collect{ it.getFilename() } as Set)
        then:
        fileNames == ["part0001", "part0002"] as Set
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
        def metaIndex = storageManagement.getMetaIndexForDelivery("test_delivery3", "version4")
        def indexInfos = storageManagement.getIndexInfoForDelivery(metaIndex)
        def stream = storageManagement.getInputStream(indexInfos[0])
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
        def metaData = storageManagement.getMetaDataForDelivery("test_delivery3", "version4", true)
        def dataInfos = storageManagement.getDataInfoForDelivery(metaData)
        def stream = storageManagement.getInputStream(dataInfos[0])
        then:
        IOUtils.toString(stream) == "The real data"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "activateChunkedVersionForAllCollections should activate the version for all available collections"() {
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
        then:
        storageManagement.getActiveDeliveryVersion("test_collection1", "test_delivery1") == "version1"
        when:
        storageManagement.activateChunkedVersion("test_delivery1", "version2")
        then:
        storageManagement.getActiveDeliveryVersion("test_collection1", "test_delivery1") == "version2"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "activateChunkedVersionInCollection should activate the version for the given collection"() {
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
        storageManagement.activateChunkedVersionInCollection("test_collection1", "test_delivery1", "version1")
        then:
        storageManagement.getActiveDeliveryVersion("test_collection1", "test_delivery1") == "version1"
        when:
        storageManagement.activateChunkedVersionInCollection("test_collection1", "test_delivery1", "version2")
        then:
        storageManagement.getActiveDeliveryVersion("test_collection1", "test_delivery1") == "version2"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "deleteCompleteCollection should delete a collection with all versions and chunks"() {
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
        storageManagement.deleteCompleteCollection("test_collection1")
        def collections = storageManagement.getJumboCollections()
        then:
        collections.size() == 2
        collections[0].getName() == "test_collection2"
        collections[1].getName() == "test_collection3"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "deleteChunkedVersionForAllCollections should delete all collections inside the delivery version"() {
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

    def "deleteChunkedVersionInCollection should delete only a chunked version for the collection"() {
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
        storageManagement.deleteChunkedVersionInCollection("test_collection1", "test_delivery1", "version1")
        def collections = storageManagement.getJumboCollections()
        then: "deletes version1 of collection1"
        collections.size() == 3
        collections[0].getName() == "test_collection1"
        collections[0].getChunks().size() == 2
        collections[0].getChunks()[0].getKey() == "test_delivery1"
        collections[0].getChunks()[0].getVersions().size() == 1
        collections[0].getChunks()[0].getVersions()[0].getVersion() == "version2"
        collections[1].getName() == "test_collection2"
        collections[2].getName() == "test_collection3"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def createDefaultFileStructure(File dataDir, File indexDir) {
        makeDataCollection(dataDir, "test_collection1", "test_delivery1", "version1", "2013-12-01")
        makeDataCollection(dataDir, "test_collection1", "test_delivery1", "version2", "2013-12-02")
        activateVersion(dataDir, "test_collection1", "test_delivery1", "version2")

        makeDataCollection(dataDir, "test_collection1", "test_delivery2", "version3", "2013-12-03")
        activateVersion(dataDir, "test_collection1", "test_delivery2", "version3")

        makeDataCollection(dataDir, "test_collection2", "test_delivery1", "version1", "2013-12-01") // yes thats correct version1
        activateVersion(dataDir, "test_collection2", "test_delivery1", "version1")

        makeDataCollection(dataDir, "test_collection3", "test_delivery3", "version4", "2013-12-04")
        makeIndex(indexDir, "test_collection3", "test_delivery3", "version4", "test_index1")
        makeIndex(indexDir, "test_collection3", "test_delivery3", "version4", "test_index2")
        activateVersion(dataDir, "test_collection3", "test_delivery3", "version4")
    }

    def activateVersion(dataDir, collection, deliveryKey, version) {
        def propsFile = new File(dataDir.getAbsolutePath() + "/$collection/$deliveryKey/" + ActiveProperties.DEFAULT_FILENAME)
        Files.createParentDirs(propsFile)
        ActiveProperties.writeActiveFile(propsFile, version)

    }

    def makeDataCollection(dataDir, collection, deliveryKey, version, date) {
        def sdf = new SimpleDateFormat("yyyy-MM-dd")
        def path = dataDir.getAbsolutePath() + "/$collection/$deliveryKey/$version/"
        def bytes = "The real data".getBytes("UTF-8")
        SnappyChunksUtil.copy(new ByteArrayInputStream(bytes), new File(path + "part0001"), bytes.length, 32768)
        SnappyChunksUtil.copy(new ByteArrayInputStream(bytes), new File(path + "part0002"), bytes.length, 32768)
        def propsFile = new File(path + CollectionProperties.DEFAULT_FILENAME)
        Files.createParentDirs(propsFile)
        def meta = new CollectionProperties.CollectionMeta(version, "some info", sdf.parse(date), "source path", "test_data_strategy")
        CollectionProperties.write(propsFile, meta)
    }

    def makeIndex(indexDir, collection, deliveryKey, version, indexName) {
        def path = indexDir.getAbsolutePath() + "/$collection/$deliveryKey/$version/$indexName/"
        def bytes = "Hello World".getBytes("UTF-8")
        SnappyChunksUtil.copy(new ByteArrayInputStream(bytes), new File(path + "part0001.odx"), bytes.length, 32768)
        SnappyChunksUtil.copy(new ByteArrayInputStream(bytes), new File(path + "part0002.odx"), bytes.length, 32768)
        def propsFile = new File(path + IndexProperties.DEFAULT_FILENAME)
        Files.createParentDirs(propsFile)
        def meta = new IndexProperties.IndexMeta(version, new Date(), indexName, "test_index_strategy", "source fields")
        IndexProperties.write(propsFile, meta)
    }
}
