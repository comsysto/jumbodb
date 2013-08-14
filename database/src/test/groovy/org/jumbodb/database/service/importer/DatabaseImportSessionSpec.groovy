package org.jumbodb.database.service.importer

import org.jumbodb.database.service.query.DatabaseQuerySession
import org.xerial.snappy.SnappyInputStream
import org.xerial.snappy.SnappyOutputStream
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class DatabaseImportSessionSpec extends Specification {
    def "import collection data"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:import:collection:data") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeUTF("file_name_part001") // send collection
        cmds.writeLong(1234567l) // send file length
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("TEST_STRATEGY")
        def snappyOut = new SnappyOutputStream(cmdStream)
        snappyOut.write("Hello World".getBytes("UTF-8"))
        snappyOut.flush()
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        def dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        then:
        dataInputStream.readInt() == DatabaseImportSession.PROTOCOL_VERSION

        1 * importHandlerMock.onImport(_, _) >> { info, importInputStream ->
            assert importInputStream != null
            def bytes = new byte[11]
            importInputStream.read(bytes)
            assert new String(bytes) == "Hello World"
            assert info.getFileType() == ImportMetaFileInformation.FileType.DATA
            assert info.getCollection() == "test_collection"
            assert info.getIndexName() == null
            assert info.getFileLength() == 1234567l
            assert info.getDeliveryVersion() == "test_delivery_version"
            assert info.getDeliveryKey() == "test_delivery_key"
            assert info.getStrategy() == "TEST_STRATEGY"
            assert info.getFileName() == "file_name_part001"
        }
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "import collection index"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:import:collection:index") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeUTF("test_index") // send index name
        cmds.writeUTF("file_name_part001") // send file
        cmds.writeLong(1234567l) // send file length
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("TEST_STRATEGY")
        def snappyOut = new SnappyOutputStream(cmdStream)
        snappyOut.write("Hello World".getBytes("UTF-8"))
        snappyOut.flush()
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        def dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        then:
        dataInputStream.readInt() == DatabaseImportSession.PROTOCOL_VERSION

        1 * importHandlerMock.onImport(_, _) >> { info, importInputStream ->
            assert importInputStream != null
            def bytes = new byte[11]
            importInputStream.read(bytes)
            assert new String(bytes) == "Hello World"
            assert info.getFileType() == ImportMetaFileInformation.FileType.INDEX
            assert info.getCollection() == "test_collection"
            assert info.getIndexName() == "test_index"
            assert info.getFileLength() == 1234567l
            assert info.getDeliveryVersion() == "test_delivery_version"
            assert info.getDeliveryKey() == "test_delivery_key"
            assert info.getStrategy() == "TEST_STRATEGY"
            assert info.getFileName() == "file_name_part001"
        }
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "collection meta data without activation"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:import:collection:meta:data") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("TEST_STRATEGY")
        cmds.writeUTF("source path")
        cmds.writeBoolean(false)
        cmds.writeUTF("some info")
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        def dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        then:
        dataInputStream.readInt() == DatabaseImportSession.PROTOCOL_VERSION

        1 * importHandlerMock.onCollectionMetaData(_) >> { metaDatas ->
            def metaData = metaDatas[0]
            assert metaData.getCollection() == "test_collection"
            assert metaData.getDeliveryVersion() == "test_delivery_version"
            assert metaData.getDeliveryKey() == "test_delivery_key"
            assert metaData.getDataStrategy() == "TEST_STRATEGY"
            assert metaData.getSourcePath() == "source path"
            assert metaData.getInfo() == "some info"
        }
        0 * importHandlerMock.onActivateDelivery(_)
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "collection meta data with activation"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:import:collection:meta:data") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("TEST_STRATEGY")
        cmds.writeUTF("source path")
        cmds.writeBoolean(true)
        cmds.writeUTF("some info")
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        def dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        then:
        dataInputStream.readInt() == DatabaseImportSession.PROTOCOL_VERSION
        1 * importHandlerMock.onCollectionMetaData(_)
        1 * importHandlerMock.onActivateDelivery(_) >> { metaDatas ->
            def metaData = metaDatas[0]
            assert metaData.getCollection() == "test_collection"
            assert metaData.getDeliveryVersion() == "test_delivery_version"
            assert metaData.getDeliveryKey() == "test_delivery_key"
            assert metaData.getDataStrategy() == "TEST_STRATEGY"
            assert metaData.getSourcePath() == "source path"
            assert metaData.getInfo() == "some info"
        }
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "collection meta index"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:import:collection:meta:index") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("test_index_name")
        cmds.writeUTF("TEST_STRATEGY")
        cmds.writeUTF("index source fields")
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        def dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        then:
        dataInputStream.readInt() == DatabaseImportSession.PROTOCOL_VERSION

        1 * importHandlerMock.onCollectionMetaIndex(_) >> { metaIndexes ->
            def metaIndex = metaIndexes[0]
            assert metaIndex.getCollection() == "test_collection"
            assert metaIndex.getDeliveryVersion() == "test_delivery_version"
            assert metaIndex.getDeliveryKey() == "test_delivery_key"
            assert metaIndex.getStrategy() == "TEST_STRATEGY"
            assert metaIndex.getIndexName() == "test_index_name"
            assert metaIndex.getIndexSourceFields() == "index source fields"
        }
        0 * importHandlerMock.onActivateDelivery(_)
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "import finished"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:import:finished") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        def dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        then:
        dataInputStream.readInt() == DatabaseImportSession.PROTOCOL_VERSION
        1 * importHandlerMock.onFinished("test_delivery_key", "test_delivery_version")
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }


    def "existsDeliveryVersion should be true"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:import:delivery:version:exists") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        def dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        then:
        dataInputStream.readInt() == DatabaseImportSession.PROTOCOL_VERSION
        dataInputStream.readBoolean() == true
        1 * importHandlerMock.existsDeliveryVersion("test_delivery_key", "test_delivery_version") >> true
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "existsDeliveryVersion should be false"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:import:delivery:version:exists") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        def dataInputStream = new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        then:
        dataInputStream.readInt() == DatabaseImportSession.PROTOCOL_VERSION
        dataInputStream.readBoolean() == false
        1 * importHandlerMock.existsDeliveryVersion("test_delivery_key", "test_delivery_version") >> false
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "unsupported operation"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeUTF(":cmd:is:not:supported") // send command
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def importHandlerMock = Mock(ImportHandler)
        def session = new DatabaseImportSession(socketMock, 1)
        when:
        session.runImport(importHandlerMock)
        then:
        thrown UnsupportedOperationException
        cleanup:
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }
}
