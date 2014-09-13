package org.jumbodb.database.service.query

import org.apache.commons.io.IOUtils
import org.apache.commons.lang.UnhandledException
import org.jumbodb.connector.JumboConstants
import org.xerial.snappy.SnappyCodec
import org.xerial.snappy.SnappyInputStream
import org.xerial.snappy.SnappyOutputStream
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
// CARSTEN test SQL query
class DatabaseQuerySessionSpec extends Specification {

    def "command query with result"() {
        setup:
        def query = '{"sample": "query"}'
        def cmdStream = new ByteArrayOutputStream()
        def snappyOutputStream = new SnappyOutputStream(cmdStream)
        def cmds = new DataOutputStream(snappyOutputStream)
        cmds.writeInt(JumboConstants.QUERY_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:query") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeInt(query.length()) // send length
        cmds.write(query.getBytes("UTF-8")) // send query
        cmds.flush()
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def queryHandlerMock = Mock(DatabaseQuerySession.QueryHandler)
        def session = new DatabaseQuerySession(socketMock, 1)
        when:
        session.query(queryHandlerMock)
        def dataInputStream = new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(outputStream.toByteArray())))
        then:
        1 * queryHandlerMock.onQuery("test_collection", query.getBytes("UTF-8"), _) >> { col, bytes, writer ->
            writer.writeResult("Hello World 1".getBytes("UTF-8"))
            writer.writeResult("Hello World 2".getBytes("UTF-8"))
            return 2
        }
        def expected1 = "Hello World 1"
        def expected1Len = expected1.getBytes("UTF-8").length
        def bytes = new byte[expected1Len]
        dataInputStream.readInt() == expected1Len
        dataInputStream.read(bytes)
        new String(bytes, "UTF-8") == expected1

        def expected2 = "Hello World 2"
        def expected2Len = expected1.getBytes("UTF-8").length
        def bytes2 = new byte[expected2Len]
        dataInputStream.readInt() == expected2Len
        dataInputStream.read(bytes2)
        new String(bytes2, "UTF-8") == expected2

        dataInputStream.readInt() == -1
        dataInputStream.readUTF() == ":result:end"
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "collection missing exception"() {
        setup:
        def query = '{"sample": "query"}'
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(new SnappyOutputStream(cmdStream))
        cmds.writeInt(JumboConstants.QUERY_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:query") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeInt(query.length()) // send length
        cmds.write(query.getBytes("UTF-8")) // send query
        cmds.flush()
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def queryHandlerMock = Mock(DatabaseQuerySession.QueryHandler)
        def session = new DatabaseQuerySession(socketMock, 1)
        when:
        session.query(queryHandlerMock)
        def dataInputStream = new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(outputStream.toByteArray())))
        then:
        1 * queryHandlerMock.onQuery("test_collection", query.getBytes("UTF-8"), _) >> {
            throw new JumboCollectionMissingException("Collection is missing")
        }
        dataInputStream.readInt() == -1
        dataInputStream.readUTF() == ":error:collection:missing"
        dataInputStream.readUTF() == "Collection is missing"
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }


    def "index missing exception"() {
        setup:
        def query = '{"sample": "query"}'
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(new SnappyOutputStream(cmdStream))
        cmds.writeInt(JumboConstants.QUERY_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:query") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeInt(query.length()) // send length
        cmds.write(query.getBytes("UTF-8")) // send query
        cmds.flush()
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def queryHandlerMock = Mock(DatabaseQuerySession.QueryHandler)
        def session = new DatabaseQuerySession(socketMock, 1)
        when:
        session.query(queryHandlerMock)
        def dataInputStream = new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(outputStream.toByteArray())))
        then:
        1 * queryHandlerMock.onQuery("test_collection", query.getBytes("UTF-8"), _) >> {
            throw new JumboIndexMissingException("Index is missing")
        }
        dataInputStream.readInt() == -1
        dataInputStream.readUTF() == ":error:collection:index:missing"
        dataInputStream.readUTF() == "Index is missing"
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "common exception"() {
        setup:
        def query = '{"sample": "query"}'
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(new SnappyOutputStream(cmdStream))
        cmds.writeInt(JumboConstants.QUERY_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:query") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeInt(query.length()) // send length
        cmds.write(query.getBytes("UTF-8")) // send query
        cmds.flush()
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def queryHandlerMock = Mock(DatabaseQuerySession.QueryHandler)
        def session = new DatabaseQuerySession(socketMock, 1)
        when:
        session.query(queryHandlerMock)
        def dataInputStream = new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(outputStream.toByteArray())))
        then:
        1 * queryHandlerMock.onQuery("test_collection", query.getBytes("UTF-8"), _) >> {
            throw new JumboCommonException("Common exception")
        }
        dataInputStream.readInt() == -1
        dataInputStream.readUTF() == ":error:common"
        dataInputStream.readUTF() == "Common exception"
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "eof exception"() {
        setup:
        def value = new ByteArrayOutputStream()
        def snappy = new SnappyOutputStream(value)
        snappy.flush()

        def inputStream = new ByteArrayInputStream(value.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def queryHandlerMock = Mock(DatabaseQuerySession.QueryHandler)
        def session = new DatabaseQuerySession(socketMock, 1)
        when:
        session.query(queryHandlerMock)
        def dataInputStream = new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(outputStream.toByteArray())))
        then:
        0 * queryHandlerMock.onQuery(_, _, _)
        dataInputStream.available() == 0
        cleanup:
        dataInputStream.close()
        inputStream.close()
        outputStream.close()
        session.close()

    }

    def "unknown exception"() {
        setup:
        def query = '{"sample": "query"}'
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(new SnappyOutputStream(cmdStream))
        cmds.writeInt(JumboConstants.QUERY_PROTOCOL_VERSION)
        cmds.writeUTF(":cmd:query") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeInt(query.length()) // send length
        cmds.write(query.getBytes("UTF-8")) // send query
        cmds.flush()
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def queryHandlerMock = Mock(DatabaseQuerySession.QueryHandler)
        def session = new DatabaseQuerySession(socketMock, 1)
        when:
        session.query(queryHandlerMock)
        def dataInputStream = new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(outputStream.toByteArray())))
        then:
        1 * queryHandlerMock.onQuery("test_collection", query.getBytes("UTF-8"), _) >> {
            throw new RuntimeException("Unhandled")
        }
        dataInputStream.readInt() == -1
        dataInputStream.readUTF() == ":error:unknown"
        dataInputStream.readUTF().startsWith("An unknown error occured on server side, check database log for further information:")
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }

    def "wrong version exception"() {
        setup:
        def query = '{"sample": "query"}'
        def cmdStream = new ByteArrayOutputStream()
        def cmds = new DataOutputStream(new SnappyOutputStream(cmdStream))
        cmds.writeInt(Integer.MAX_VALUE) // wrong version
        cmds.writeUTF(":cmd:query") // send command
        cmds.writeUTF("test_collection") // send collection
        cmds.writeInt(query.length()) // send length
        cmds.write(query.getBytes("UTF-8")) // send query
        cmds.flush()
        def inputStream = new ByteArrayInputStream(cmdStream.toByteArray())
        def outputStream = new ByteArrayOutputStream()
        def socketMock = Mock(Socket)
        socketMock.getInputStream() >> inputStream
        socketMock.getOutputStream() >> outputStream
        def queryHandlerMock = Mock(DatabaseQuerySession.QueryHandler)
        def session = new DatabaseQuerySession(socketMock, 1)
        when:
        session.query(queryHandlerMock)
        def dataInputStream = new DataInputStream(new SnappyInputStream(new ByteArrayInputStream(outputStream.toByteArray())))
        then:
        0 * queryHandlerMock.onQuery(_, _, _)
        dataInputStream.readInt() == -1
        dataInputStream.readUTF() == ":error:wrongversion"
        dataInputStream.readUTF().startsWith("Wrong protocol version. Got 2147483647, but expected")
        cleanup:
        dataInputStream.close()
        cmds.close()
        inputStream.close()
        outputStream.close()
        session.close()
    }
}
