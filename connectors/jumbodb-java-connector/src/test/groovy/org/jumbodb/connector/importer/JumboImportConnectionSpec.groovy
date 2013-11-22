package org.jumbodb.connector.importer

import org.apache.commons.io.IOUtils
import org.jumbodb.connector.JumboConstants
import org.xerial.snappy.SnappyInputStream
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class JumboImportConnectionSpec extends Specification {
    JumboImportConnection jis
    ServerSocket serverSocket
    Socket clientSocket
    InputStream is
    DataInputStream dis
    DataOutputStream dos

    def setup() {
        serverSocket = new ServerSocket(12001);
        Thread.start {
            clientSocket = serverSocket.accept();
            is = clientSocket.getInputStream()
            dis = new DataInputStream(is)
            dos = new DataOutputStream(clientSocket.getOutputStream())
            dos.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION)
            dos.flush()
        }
        jis = new JumboImportConnection("localhost", 12001)
    }

    def cleanup() {
        jis.close()
        clientSocket.close()
        serverSocket.close()
    }

    def "exists delivery version #deliveryExists"() {
        expect:
        Thread.start {
            assert dis.readUTF() == ":cmd:import:delivery:version:exists"
            assert dis.readUTF() == "my_chunk"
            assert dis.readUTF() == "my_version"
            dos.writeBoolean(deliveryExists)
            dos.flush()
        }
        jis.existsDeliveryVersion("my_chunk", "my_version") == deliveryExists
        where:
        deliveryExists << [true, false]
    }

    def "import index with valid SHA1 hash"() {
        setup:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def indexInfo = new IndexInfo("my_collection", "my_index", "part0005", dataToSend.length, "my_delivery", "my_version", "INDEX_STRATEGY")
        def copyCallBackMock = new OnCopyCallback() {
            @Override
            String onCopy(OutputStream outputStream) {
                IOUtils.write(dataToSend, outputStream)
                outputStream.flush()
                return "this_is_the_expected_hash"
            }
        }
        expect:
        Thread.start {
            assert dis.readUTF() == ":cmd:import:collection:index"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "my_index"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "INDEX_STRATEGY"
            def sis = new SnappyInputStream(is)
            def data = IOUtils.toByteArray(sis, dataToSend.length)
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.writeUTF("this_is_the_expected_hash")
            dos.flush()
        }
        jis.importIndex(indexInfo, copyCallBackMock)
    }

    def "import index with invalid SHA1 hash"() {
        when:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def indexInfo = new IndexInfo("my_collection", "my_index", "part0005", dataToSend.length, "my_delivery", "my_version", "INDEX_STRATEGY")
        def copyCallBackMock = new OnCopyCallback() {
            @Override
            String onCopy(OutputStream outputStream) {
                IOUtils.write(dataToSend, outputStream)
                outputStream.flush()
                return "this_is_the_expected_hash"
            }
        }
        Thread.start {
            assert dis.readUTF() == ":cmd:import:collection:index"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "my_index"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "INDEX_STRATEGY"
            def sis = new SnappyInputStream(is)
            def data = IOUtils.toByteArray(sis, dataToSend.length)
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.writeUTF("this_is_an_invalid_hash")
            dos.flush()
        }
        jis.importIndex(indexInfo, copyCallBackMock)
        then:
        thrown InvalidFileHashException
    }

    def "import data with valid SHA1 hash"() {
        setup:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def dataInfo = new DataInfo("my_collection", "part0005", dataToSend.length, "my_delivery", "my_version", "DATA_STRATEGY")
        def copyCallBackMock = new OnCopyCallback() {
            @Override
            String onCopy(OutputStream outputStream) {
                IOUtils.write(dataToSend, outputStream)
                outputStream.flush()
                return "this_is_the_expected_hash"
            }
        }
        expect:
        Thread.start {
            assert dis.readUTF() == ":cmd:import:collection:data"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "DATA_STRATEGY"
            def sis = new SnappyInputStream(is)
            def data = IOUtils.toByteArray(sis, dataToSend.length)
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.writeUTF("this_is_the_expected_hash")
            dos.flush()
        }
        jis.importData(dataInfo, copyCallBackMock)
        jis.getByteCount() == 205 // data length + meta data
    }

    def "import data with invalid SHA1 hash"() {
        when:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def dataInfo = new DataInfo("my_collection", "part0005", dataToSend.length, "my_delivery", "my_version", "DATA_STRATEGY")
        def copyCallBackMock = new OnCopyCallback() {
            @Override
            String onCopy(OutputStream outputStream) {
                IOUtils.write(dataToSend, outputStream)
                outputStream.flush()
                return "this_is_the_expected_hash"
            }
        }
        Thread.start {
            assert dis.readUTF() == ":cmd:import:collection:data"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "DATA_STRATEGY"
            def sis = new SnappyInputStream(is)
            def data = IOUtils.toByteArray(sis, dataToSend.length)
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.writeUTF("this_is_an_invalid_hash")
            dos.flush()
        }
        jis.importData(dataInfo, copyCallBackMock)
        then:
        thrown InvalidFileHashException
    }

    def "send meta info index"() {
        setup:
        def metaInfo = new MetaIndex("my_collection", "my_delivery", "my_version", "my_index", "MY_INDEX_STRATEGY", "the defined source fields")
        expect:
        Thread.start {
            assert dis.readUTF() == ":cmd:import:collection:meta:index"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "my_index"
            assert dis.readUTF() == "MY_INDEX_STRATEGY"
            assert dis.readUTF() == "the defined source fields"
        }
        jis.sendMetaIndex(metaInfo)
    }

    def "send meta info data "() {
        setup:
        def metaInfo = new MetaData("my_collection", "my_delivery", "my_version", "MY_DATA_STRATEGY", "A path", true, "some additional info")
        expect:
        Thread.start {
            assert dis.readUTF() == ":cmd:import:collection:meta:data"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "MY_DATA_STRATEGY"
            assert dis.readUTF() == "A path"
            assert dis.readBoolean()
            assert dis.readUTF() == "some additional info"
        }
        jis.sendMetaData(metaInfo)
    }

    def "sendFinishedNotification"() {
        expect:
        jis.sendFinishedNotification("my_delivery", "my_version")
        assert dis.readUTF() == ":cmd:import:finished"
        assert dis.readUTF() == "my_delivery"
        assert dis.readUTF() == "my_version"
    }
}
