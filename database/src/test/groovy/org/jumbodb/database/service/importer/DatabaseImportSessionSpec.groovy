package org.jumbodb.database.service.importer

import org.jumbodb.common.query.ChecksumType
import org.jumbodb.connector.JumboConstants
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class DatabaseImportSessionSpec extends Specification {
    def "import collection data"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        def sendBytes = "Hello World".getBytes("UTF-8")
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:import:collection:data") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("test_collection")
        cmds.writeUTF("file_name_part001")
        cmds.writeLong(sendBytes.length)
        cmds.writeUTF(ChecksumType.NONE.toString())
        cmds.write(sendBytes)
        cmds.flush()
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
        1 * importHandlerMock.onImport(_, _) >> { info, importInputStream ->
            assert importInputStream != null
            def bytes = new byte[11]
            importInputStream.read(bytes)
            assert new String(bytes) == "Hello World"
            assert info.getFileType() == ImportMetaFileInformation.FileType.DATA
            assert info.getCollection() == "test_collection"
            assert info.getIndexName() == null
            assert info.getFileLength() == 11l
            assert info.getDeliveryVersion() == "test_delivery_version"
            assert info.getDeliveryKey() == "test_delivery_key"
            assert info.getFileName() == "file_name_part001"
        }
        dataInputStream.readUTF() == ":success"
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
        def sendBytes = "Hello World".getBytes("UTF-8")
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:import:collection:index") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("test_collection") // send collection
        cmds.writeUTF("test_index") // send index name
        cmds.writeUTF("file_name_part001") // send file
        cmds.writeLong(sendBytes.length) // send file length
        cmds.writeUTF(ChecksumType.NONE.toString())
        cmds.write(sendBytes)
        cmds.flush()
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
        1 * importHandlerMock.onImport(_, _) >> { info, importInputStream ->
            assert importInputStream != null
            def bytes = new byte[11]
            importInputStream.read(bytes)
            assert new String(bytes) == "Hello World"
            assert info.getFileType() == ImportMetaFileInformation.FileType.INDEX
            assert info.getCollection() == "test_collection"
            assert info.getIndexName() == "test_index"
            assert info.getFileLength() == 11l
            assert info.getDeliveryVersion() == "test_delivery_version"
            assert info.getDeliveryKey() == "test_delivery_key"
            assert info.getFileName() == "file_name_part001"
        }
        dataInputStream.readUTF() == ":success"
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "import initialization"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:import:init") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("2012-12-12 12:12:12") // send collection
        cmds.writeUTF("some info")
        cmds.flush()
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
        1 * importHandlerMock.onInit("test_delivery_key", "test_delivery_version", "2012-12-12 12:12:12", "some info")
        dataInputStream.readUTF() == ":success"
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "import initialization with existing delivery version should fail"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:import:init") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeUTF("2012-12-12 12:12:12") // send collection
        cmds.writeUTF("some info")
        cmds.flush()
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
        1 * importHandlerMock.onInit("test_delivery_key", "test_delivery_version", "2012-12-12 12:12:12", "some info") >> {
            throw new DeliveryVersionExistsException("my error")
        }
        dataInputStream.readUTF() == ":error:deliveryversionexists"
        dataInputStream.readUTF() == "my error"
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "import commit with activation"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:import:commit") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeBoolean(true)
        cmds.writeBoolean(true)
        cmds.flush()
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
        1 * importHandlerMock.onCommit("test_delivery_key", "test_delivery_version", true, true)
        dataInputStream.readUTF() == ":success"
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }


    def "import commit without activation"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:import:commit") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.writeBoolean(false)
        cmds.writeBoolean(false)
        cmds.flush()
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
        1 * importHandlerMock.onCommit("test_delivery_key", "test_delivery_version", false, false)
        dataInputStream.readUTF() == ":success"
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
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:import:delivery:version:exists") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.flush()
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
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:import:delivery:version:exists") // send command
        cmds.writeUTF("test_delivery_key")
        cmds.writeUTF("test_delivery_version")
        cmds.flush()
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
        cmds.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:is:not:supported") // send command
        cmds.flush()
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

    def "wrong protocol version"() {
        setup:
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(cmdStream)
        cmds.writeInt(Integer.MAX_VALUE) // send wrong version
        cmds.writeUTF(":cmd:is:not:supported") // send command
        cmds.flush()
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
        dataInputStream.readUTF() == ":error:wrongversion"
        dataInputStream.readUTF().startsWith("Wrong protocol version - Got 2147483647, but expected ")
        cleanup:
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }
}
