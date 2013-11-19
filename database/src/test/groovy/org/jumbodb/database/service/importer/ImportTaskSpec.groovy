package org.jumbodb.database.service.importer

import com.google.common.io.Files
import org.jumbodb.database.service.query.JumboSearcher
import org.jumbodb.database.service.query.data.DataStrategy
import org.jumbodb.database.service.query.data.DataStrategyManager
import org.jumbodb.database.service.query.index.IndexStrategy
import org.jumbodb.database.service.query.index.IndexStrategyManager
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class ImportTaskSpec extends Specification {
    def "run onImport data"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)
        def dataStrategyMock = Mock(DataStrategy)
        def metaInfo = new ImportMetaFileInformation(ImportMetaFileInformation.FileType.DATA, "testFileName", "test_collection", null, 1234567l, "test_delivery_key", "test_version", "test_strategy")

        def inputStreamMock = Mock(InputStream)
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            assert importHandler[0].onImport(metaInfo, inputStreamMock) == "sha1hash"
        }
        1 * dataStrategyManagerMock.getStrategy("test_strategy") >> dataStrategyMock
        1 * dataStrategyMock.onImport(metaInfo, inputStreamMock, new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/import/test_collection")) >> "sha1hash"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "run onImport index"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)
        def indexStrategyMock = Mock(IndexStrategy)
        def metaInfo = new ImportMetaFileInformation(ImportMetaFileInformation.FileType.INDEX, "testFileName", "test_collection", "test_index_name", 1234567l, "test_delivery_key", "test_version", "test_strategy")
        def inputStreamMock = Mock(InputStream)
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            assert importHandler[0].onImport(metaInfo, inputStreamMock) == "sha1hash"
        }
        1 * indexStrategyManagerMock.getStrategy("test_strategy") >> indexStrategyMock
        1 * indexStrategyMock.onImport(metaInfo, inputStreamMock, new File(indexDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/import/test_collection/test_index_name")) >> "sha1hash"
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "existsDeliveryVersion should be false"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            assert importHandler[0].existsDeliveryVersion("test_delivery_key", "test_version") == false
        }
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "existsDeliveryVersion should be true"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        new File(dataDir.getAbsolutePath() + "/a_collection/test_delivery_key/test_version").mkdirs()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            assert importHandler[0].existsDeliveryVersion("test_delivery_key", "test_version") == true
        }
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }


    def "onCollectionMetaData no existing collection, force activation"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)
        def importMetaData = new ImportMetaData("test_collection", "test_delivery_key", "test_version", "test_strategy", "test source path", "test info")
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onCollectionMetaData(importMetaData)
        }
        new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/import/test_collection/delivery.properties").exists() == true
        new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/activate/test_collection").exists() == true
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "onCollectionMetaData with existing collection"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)
        def importMetaData = new ImportMetaData("test_collection", "test_delivery_key", "test_version", "test_strategy", "test source path", "test info")
        new File(dataDir.getAbsolutePath() + "/test_collection/test_delivery_key/test_version/").mkdirs()
        new File(dataDir.getAbsolutePath() + "/test_collection/test_delivery_key/active.properties").createNewFile()
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onCollectionMetaData(importMetaData)
        }
        new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/import/test_collection/delivery.properties").exists() == true
        new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/activate/test_collection").exists() == false
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }


    def "onCollectionMetaIndex"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)
        def importMetaIndex = new ImportMetaIndex("test_collection", "test_delivery_key", "test_version", "test_index_name", "test_strategy", "source fields")
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onCollectionMetaIndex(importMetaIndex)
        }
        new File(indexDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/import/test_collection/test_index_name/index.properties").exists() == true
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "onActivateDelivery"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)
        def importMetaData = new ImportMetaData("test_collection", "test_delivery_key", "test_version", "test_strategy", "test source path", "test info")
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onActivateDelivery(importMetaData)
        }
        new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/activate/test_collection").exists() == true
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "onFinished"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, dataStrategyManagerMock, indexStrategyManagerMock, importSessionMock)

        // creating temp folders
        def tmpActivationFile = new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/activate/test_collection")
        Files.createParentDirs(tmpActivationFile)
        tmpActivationFile.createNewFile()
        def tmpIndexFile = new File(indexDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/import/test_collection/test_index_name/index.properties")
        Files.createParentDirs(tmpIndexFile)
        tmpIndexFile.createNewFile()
        def tmpDataFile = new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key_test_version/import/test_collection/delivery.properties")
        Files.createParentDirs(tmpDataFile)
        tmpDataFile.createNewFile()
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onFinished("test_delivery_key", "test_version")
        }
        1 * jumboSearcherMock.onDataChanged()
        new File(dataDir.getAbsolutePath() + "/test_collection/test_delivery_key/active.properties").exists() == true
        tmpActivationFile.exists() == false
        new File(indexDir.getAbsolutePath() + "/test_collection/test_delivery_key/test_version/test_index_name/index.properties").exists() == true
        tmpIndexFile.exists() == false
        new File(dataDir.getAbsolutePath() + "/test_collection/test_delivery_key/test_version/delivery.properties").exists() == true
        tmpDataFile.exists() == false
        cleanup:
        dataDir.delete()
        indexDir.delete()

    }

    class ImportTaskWithSessionMock extends ImportTask {
        private DatabaseImportSession session

        ImportTaskWithSessionMock(Socket s, File dataDir, File indexDir, JumboSearcher jumboSearcher, DataStrategyManager dataStrategyManager, IndexStrategyManager indexStrategyManager, DatabaseImportSession session) {
            super(s, 5, dataDir, indexDir, jumboSearcher, dataStrategyManager, indexStrategyManager)
            this.session = session
        }

        @Override
        protected DatabaseImportSession createDatabaseImportSession() throws IOException {
            return session
        }
    }

}
