package org.jumbodb.connector.importer

import org.apache.commons.io.IOUtils
import org.jumbodb.connector.JumboConstants
import org.jumbodb.connector.exception.JumboWrongVersionException
import org.xerial.snappy.SnappyInputStream
import org.xerial.snappy.SnappyOutputStream
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
// CARSTEN fix tests
class JumboImportConnectionSpec extends Specification {
    JumboImportConnection jis
    ServerSocket serverSocket
    Socket clientSocket
    InputStream is
    SnappyInputStream sis
    DataInputStream dis
    OutputStream os
    SnappyOutputStream sos
    DataOutputStream dos


    def setup() {
        serverSocket = new ServerSocket(12001);
        Thread.start {
            clientSocket = serverSocket.accept();
            os = clientSocket.getOutputStream()
            sos = new SnappyOutputStream(os)
            dos = new DataOutputStream(sos)
            dos.flush()
            is = clientSocket.getInputStream()
            sis = new SnappyInputStream(is)
            dis = new DataInputStream(sis)
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
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
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

    def "import index with valid MD5 hash"() {
        setup:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def indexInfo = new IndexInfo("my_delivery", "my_version", "my_collection", "my_index", "part0005", dataToSend.length, "INDEX_STRATEGY")
        def copyCallBackMock = new OnCopyCallback() {
            @Override
            void onCopy(OutputStream outputStream) {
                IOUtils.write(dataToSend, outputStream)
                outputStream.flush()
            }
        }
        expect:
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:collection:index"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "my_index"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "INDEX_STRATEGY"
            dos.writeUTF(":copy");
            dos.flush()
            def data = IOUtils.toByteArray(dis, dataToSend.length)
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.writeUTF(":verify:md5")
            dos.writeUTF("this_is_the_expected_hash")
            dos.flush()
        }
        jis.importIndexFile(indexInfo, copyCallBackMock)
    }

    def "import index with invalid MD5 hash"() {
        when:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def indexInfo = new IndexInfo("my_delivery", "my_version", "my_collection", "my_index", "part0005", dataToSend.length, "INDEX_STRATEGY")
        def copyCallBackMock = new OnCopyCallback() {
            @Override
            void onCopy(OutputStream outputStream) {
                IOUtils.write(dataToSend, outputStream)
                outputStream.flush()
            }
        }
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:collection:index"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "my_index"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "INDEX_STRATEGY"
            dos.writeUTF(":copy");
            dos.flush()
            def data = IOUtils.toByteArray(dis, dataToSend.length)
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.writeUTF(":verify:md5")
            dos.writeUTF("this_is_an_invalid_hash")
            dos.flush()
        }
        jis.importIndexFile(indexInfo, copyCallBackMock)
        then:
        thrown InvalidFileHashException
    }

    def "import data with valid MD5 hash"() {
        setup:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def dataInfo = new DataInfo("my_delivery", "my_version", "my_collection", "part0005", dataToSend.length, "DATA_STRATEGY")
        def copyCallBackMock = new OnCopyCallback() {
            @Override
            void onCopy(OutputStream outputStream) {
                IOUtils.write(dataToSend, outputStream)
                outputStream.flush()
            }
        }
        expect:
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:collection:data"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "DATA_STRATEGY"
            dos.writeUTF(":copy");
            dos.flush()
            def data = IOUtils.toByteArray(dis, dataToSend.length)
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.writeUTF(":verify:md5")
            dos.writeUTF("this_is_the_expected_hash")
            dos.flush()
        }
        jis.importDataFile(dataInfo, copyCallBackMock)
        jis.getByteCount() == 206 // data length + meta data
    }

    def "import data with invalid MD5 hash"() {
        when:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def dataInfo = new DataInfo("my_delivery", "my_version", "my_collection", "part0005", dataToSend.length, "DATA_STRATEGY")
        def copyCallBackMock = new OnCopyCallback() {
            @Override
            void onCopy(OutputStream outputStream) {
                IOUtils.write(dataToSend, outputStream)
                outputStream.flush()
            }
        }
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:collection:data"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "DATA_STRATEGY"
            dos.writeUTF(":copy");
            dos.flush()
            def data = IOUtils.toByteArray(dis, dataToSend.length)
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.writeUTF(":verify:md5")
            dos.writeUTF("this_is_an_invalid_hash")
            dos.flush()
        }
        jis.importDataFile(dataInfo, copyCallBackMock)
        then:
        thrown InvalidFileHashException
    }
//
//    def "send meta info index"() {
//        setup:
//        def metaInfo = new MetaIndex("my_collection", "my_delivery", "my_version", "my_index", "MY_INDEX_STRATEGY", "the defined source fields")
//        expect:
//        Thread.start {
//            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
//            assert dis.readUTF() == ":cmd:import:collection:meta:index"
//            assert dis.readUTF() == "my_collection"
//            assert dis.readUTF() == "my_delivery"
//            assert dis.readUTF() == "my_version"
//            assert dis.readUTF() == "my_index"
//            assert dis.readUTF() == "MY_INDEX_STRATEGY"
//            assert dis.readUTF() == "the defined source fields"
//            dos.writeUTF(":ok")
//            dos.flush()
//        }
//        jis.sendMetaIndex(metaInfo)
//    }
//
//    def "send meta info data "() {
//        setup:
//        def metaInfo = new MetaData("my_collection", "my_delivery", "my_version", "MY_DATA_STRATEGY", "A path", true, "some additional info")
//        expect:
//        Thread.start {
//            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
//            assert dis.readUTF() == ":cmd:import:collection:meta:data"
//            assert dis.readUTF() == "my_collection"
//            assert dis.readUTF() == "my_delivery"
//            assert dis.readUTF() == "my_version"
//            assert dis.readUTF() == "MY_DATA_STRATEGY"
//            assert dis.readUTF() == "A path"
//            assert dis.readBoolean()
//            assert dis.readUTF() == "some additional info"
//            dos.writeUTF(":ok")
//            dos.flush()
//        }
//        jis.sendMetaData(metaInfo)
//    }

    def "sendFinishedNotification"() {
        expect:
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:finished"
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            dos.writeUTF(":ok")
            dos.flush()
        }
        jis.commitImport("my_delivery", "my_version")
    }

    def "handle wrong version"() {
        when:
        Thread.start {
            dis.readInt()
            dos.writeUTF(":error:wrongversion")
            dos.writeUTF("my message")
            dos.flush()
        }
        jis.commitImport("my_delivery", "my_version")
        then:
        def ex = thrown JumboWrongVersionException
        ex.getMessage() == "my message"
    }
}
