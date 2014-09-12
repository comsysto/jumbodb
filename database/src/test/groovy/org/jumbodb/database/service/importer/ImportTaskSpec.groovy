package org.jumbodb.database.service.importer

import com.google.common.io.Files
import org.jumbodb.common.query.ChecksumType
import org.jumbodb.data.common.meta.ActiveProperties
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
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, importSessionMock)
        def metaInfo = new ImportMetaFileInformation("test_delivery_key", "test_version", "test_collection", null,
                ImportMetaFileInformation.FileType.DATA, "testFileName", 1234567l, ChecksumType.NONE, null)
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onImport(metaInfo, new ByteArrayInputStream("hello world".bytes))
            File tmpImportPathByType = importTask.getTmpImportPathByType(metaInfo);
            File file = new File(tmpImportPathByType.getAbsolutePath() + "/" + metaInfo.getFileName());
            assert file.text == "hello world"
        }
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
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, importSessionMock)
        def metaInfo = new ImportMetaFileInformation("test_delivery_key", "test_version", "test_collection","test_index_name",
                ImportMetaFileInformation.FileType.INDEX, "testFileName", 1234567l, ChecksumType.NONE, "")
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onImport(metaInfo, new ByteArrayInputStream("hello world".bytes))
            File tmpImportPathByType = importTask.getTmpImportPathByType(metaInfo);
            File file = new File(tmpImportPathByType.getAbsolutePath() + "/" + metaInfo.getFileName());
            assert file.text == "hello world"
        }
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
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, importSessionMock)
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
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        new File(dataDir.getAbsolutePath() + "/test_delivery_key/test_version/a_collection").mkdirs()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, importSessionMock)
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

    def "onInit existing version"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, importSessionMock)
        new File(dataDir.getAbsolutePath() + "/test_delivery_key/test_version/").mkdirs()
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onInit("test_delivery_key", "test_version", "2012-12-12 12:12:12", "info")
        }
        thrown(DeliveryVersionExistsException)
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }


    def "onInit version does not exist"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, importSessionMock)
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onInit("test_delivery_key", "test_version", "2012-12-12 12:12:12", "info")
        }
        new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key/test_version/").exists() == true
        new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key/test_version/delivery.properties").exists() == true
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    def "onCommit with forced activation, because first version"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, importSessionMock)

        // creating temp folders
        def tmpActivationFile = new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key/test_version/test_collection/")
        Files.createParentDirs(tmpActivationFile)
        def tmpIndexFile = new File(indexDir.getAbsolutePath() + "/.tmp/test_delivery_key/test_version/test_collection/test_index_name/index.properties")
        Files.createParentDirs(tmpIndexFile)
        tmpIndexFile.createNewFile()
        def tmpDataFile = new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key/test_version/delivery.properties")
        tmpDataFile.createNewFile()
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onCommit("test_delivery_key", "test_version", true, true)
        }
        1 * jumboSearcherMock.onDataChanged()
        new File(dataDir.getAbsolutePath() + "/test_delivery_key/active.properties").exists() == true
        tmpActivationFile.exists() == false
        new File(indexDir.getAbsolutePath() + "/test_delivery_key/test_version/test_collection/test_index_name/index.properties").exists() == true
        tmpIndexFile.exists() == false
        new File(dataDir.getAbsolutePath() + "/test_delivery_key/test_version/delivery.properties").exists() == true
        tmpDataFile.exists() == false
        cleanup:
        dataDir.delete()
        indexDir.delete()

    }

    def "onCommit no activation, existing version"() {
        setup:
        def socketMock = Mock(Socket)
        def inetAddressMock = Mock(InetAddress)
        socketMock.getInetAddress() >> inetAddressMock
        inetAddressMock.getHostName() >> "a_hostname"
        def jumboSearcherMock = Mock(JumboSearcher)
        def importSessionMock = Mock(DatabaseImportSession)
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def importTask = new ImportTaskWithSessionMock(socketMock, dataDir, indexDir, jumboSearcherMock, importSessionMock)

        // creating existing version
        def activationFile = new File(dataDir.getAbsolutePath() + "/test_delivery_key/active.properties")
        Files.createParentDirs(activationFile)
        ActiveProperties.writeActiveFile(activationFile, "oldVersion", true)
        // creating temp folders
        def tmpCollectionFolder = new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key/test_version/test_collection/")
        Files.createParentDirs(tmpCollectionFolder)
        def tmpIndexFile = new File(indexDir.getAbsolutePath() + "/.tmp/test_delivery_key/test_version/test_collection/test_index_name/index.properties")
        Files.createParentDirs(tmpIndexFile)
        tmpIndexFile.createNewFile()
        def tmpDataFile = new File(dataDir.getAbsolutePath() + "/.tmp/test_delivery_key/test_version/delivery.properties")
        tmpDataFile.createNewFile()
        when:
        importTask.run()
        then:
        1 * importSessionMock.runImport(_) >> { importHandler ->
            importHandler[0].onCommit("test_delivery_key", "test_version", false, false)
        }
        1 * jumboSearcherMock.onDataChanged()
        new File(dataDir.getAbsolutePath() + "/test_delivery_key/active.properties").exists() == true
        ActiveProperties.isDeliveryActive(activationFile) == false
        ActiveProperties.getActiveDeliveryVersion(activationFile) == "oldVersion"
        tmpCollectionFolder.exists() == false
        new File(indexDir.getAbsolutePath() + "/test_delivery_key/test_version/test_collection/test_index_name/index.properties").exists() == true
        tmpIndexFile.exists() == false
        new File(dataDir.getAbsolutePath() + "/test_delivery_key/test_version/delivery.properties").exists() == true
        tmpDataFile.exists() == false
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }

    class ImportTaskWithSessionMock extends ImportTask {
        private DatabaseImportSession session

        ImportTaskWithSessionMock(Socket s, File dataDir, File indexDir, JumboSearcher jumboSearcher, DatabaseImportSession session) {
            super(s, 5, dataDir, indexDir, jumboSearcher)
            this.session = session
        }

        @Override
        protected DatabaseImportSession createDatabaseImportSession() throws IOException {
            return session
        }
    }

}
