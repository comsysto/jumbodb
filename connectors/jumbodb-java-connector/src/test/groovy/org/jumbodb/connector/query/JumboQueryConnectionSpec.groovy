package org.jumbodb.connector.query

import org.apache.commons.io.IOUtils
import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.connector.JumboConstants
import org.jumbodb.connector.exception.JumboCommonException
import org.jumbodb.connector.exception.JumboIndexMissingException
import org.jumbodb.connector.exception.JumboTimeoutException
import org.jumbodb.connector.exception.JumboUnknownException
import org.jumbodb.connector.exception.JumboWrongVersionException
import org.jumbodb.connector.importer.JumboImportConnection
import org.xerial.snappy.SnappyInputStream
import org.xerial.snappy.SnappyOutputStream
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class JumboQueryConnectionSpec extends Specification {
    JumboQueryConnection jqc
    ServerSocket serverSocket
    Socket clientSocket
    InputStream is
    SnappyInputStream sis
    DataInputStream dis
    SnappyOutputStream sos
    DataOutputStream dos
    JumboQuery jumboQuery

    def setup() {
        jqc = new JumboQueryConnection("localhost", 12002)
        jumboQuery = new JumboQuery()
        jumboQuery.addIndexQuery("my_index", [new QueryClause(QueryOperation.EQ, "my_value")])
        serverSocket = new ServerSocket(12002);
    }

    def cleanup() {
        IOUtils.closeQuietly(clientSocket)
        IOUtils.closeQuietly(serverSocket)
    }

    def "getTimeoutInMs"() {
        expect:
        jqc.getTimeoutInMs() == 300000l
    }

    def initiateConnection() {
        clientSocket = serverSocket.accept();
        is = clientSocket.getInputStream();
        sis = new SnappyInputStream(is)
        dis = new DataInputStream(sis);
        sos = new SnappyOutputStream(clientSocket.getOutputStream());
        dos = new DataOutputStream(sos);
    }

    def "find with result"() {
        expect:
        Thread.start {
            initiateConnection()
            assert dis.readInt() == JumboConstants.QUERY_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:query"
            assert dis.readUTF() == "my_collection"
            assert dis.readInt() == 161
            def bytes = IOUtils.toByteArray(dis, 161)
            def expectedQuery = """
                {"indexQuery":[{"name":"my_index","clauses":[{"queryOperation":"EQ","value":"my_value","queryClauses":[]}]}],"jsonQuery":[],"limit":-1,"resultCacheEnabled":true}
            """
            assert IOUtils.toString(bytes, "UTF-8") == expectedQuery.trim()
            def result = """
                {"json_key": "json_value"}
            """.trim().getBytes("UTF-8")
            dos.writeInt(result.length)
            dos.write(result)
            dos.writeInt(-1)
            dos.writeUTF(":result:end")
            dos.flush()
        }
        def result = jqc.find("my_collection", Map.class, jumboQuery)
        result.size() == 1
        result.get(0).get("json_key") == "json_value"
    }

    def "findWithStreamedResult"() {
        expect:
        Thread.start {
            initiateConnection()
            assert dis.readInt() == JumboConstants.QUERY_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:query"
            assert dis.readUTF() == "my_collection"
            assert dis.readInt() == 161
            def bytes = IOUtils.toByteArray(dis, 161)
            def expectedQuery = """
                {"indexQuery":[{"name":"my_index","clauses":[{"queryOperation":"EQ","value":"my_value","queryClauses":[]}]}],"jsonQuery":[],"limit":-1,"resultCacheEnabled":true}
            """
            assert IOUtils.toString(bytes, "UTF-8") == expectedQuery.trim()
            def result = """
                {"json_key": "json_value"}
            """.trim().getBytes("UTF-8")
            dos.writeInt(result.length)
            dos.write(result)
            dos.writeInt(-1)
            dos.writeUTF(":result:end")
            dos.flush()
        }

        def result = jqc.findWithStreamedResult("my_collection", Map.class, jumboQuery)
        def it = result.iterator()
        it.hasNext()
        it.next().get("json_key") == "json_value"
        !it.hasNext()
    }

    def "find unknown error"() {
        when:
        Thread.start {
            triggerError(":error:unknown", "custom message")
        }
        jqc.find("my_collection", Map.class, jumboQuery)
        then:
        def ex = thrown JumboUnknownException
        ex.message == "custom message"
    }

    private void triggerError(String error, String message) {
        initiateConnection()
        assert dis.readInt() == JumboConstants.QUERY_PROTOCOL_VERSION
        assert dis.readUTF() == ":cmd:query"
        assert dis.readUTF() == "my_collection"
        assert dis.readInt() == 161
        IOUtils.toByteArray(dis, 161)
        dos.writeInt(-1)
        dos.writeUTF(error)
        dos.writeUTF(message)
        dos.flush()
    }

    def "find common error"() {
        when:
        Thread.start {
            triggerError(":error:common", "custom message")
        }
        jqc.find("my_collection", Map.class, jumboQuery)
        then:
        def ex = thrown JumboCommonException
        ex.message == "custom message"
    }

    def "find timeout error"() {
        when:
        Thread.start {
            triggerError(":error:timeout", "custom message")
        }
        jqc.find("my_collection", Map.class, jumboQuery)
        then:
        def ex = thrown JumboTimeoutException
        ex.message == "custom message"
    }

    def "find collection missing"() {
        when:
        Thread.start {
            triggerError(":error:collection:missing", "my_collection")
        }
        def res = jqc.find("my_collection", Map.class, jumboQuery)
        then:
        res.size() == 0
    }

    def "find index missing"() {
        when:
        Thread.start {
            triggerError(":error:collection:index:missing", "custom message")
        }
        jqc.find("my_collection", Map.class, jumboQuery)
        then:
        def ex = thrown JumboIndexMissingException
        ex.message == "custom message"
    }

    def "wrong protocol version"() {
        when:
        Thread.start {
            triggerError(":error:wrongversion", "custom message")
        }
        jqc.find("my_collection", Map.class, jumboQuery)
        then:
        def ex = thrown JumboWrongVersionException
        ex.message == "custom message"
    }

    def "find missing result end command causes exception"() {
        when:
        Thread.start {
            initiateConnection()
            assert dis.readInt() == JumboConstants.QUERY_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:query"
            assert dis.readUTF() == "my_collection"
            assert dis.readInt() == 161
            IOUtils.toByteArray(dis, 161)
            dos.writeInt(-1)
            dos.writeUTF("invalid command")
            dos.flush()
        }
        jqc.find("my_collection", Map.class, jumboQuery)
        then:
        thrown IllegalStateException

    }
}
