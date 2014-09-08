package org.jumbodb.connector.importer

import org.apache.commons.io.IOUtils
import org.jumbodb.common.query.ChecksumType
import org.jumbodb.connector.JumboConstants
import org.jumbodb.connector.exception.JumboCommonException
import org.jumbodb.connector.exception.JumboDeliveryVersionExistsException
import org.jumbodb.connector.exception.JumboFileChecksumException
import org.jumbodb.connector.exception.JumboWrongVersionException
import org.xerial.snappy.SnappyInputStream
import org.xerial.snappy.SnappyOutputStream
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
    OutputStream os
    DataOutputStream dos


    def setup() {
        serverSocket = new ServerSocket(12001);
        Thread.start {
            serverSocket.setSoTimeout(1000)
            clientSocket = serverSocket.accept();
            os = clientSocket.getOutputStream()
            dos = new DataOutputStream(os)
            is = clientSocket.getInputStream()
            dis = new DataInputStream(is)
        }
        jis = new JumboImportConnection("localhost", 12001)
        Thread.sleep(500)
    }

    def cleanup() {
        jis.close()
        clientSocket.close()
        serverSocket.close()
    }

    def "exists delivery version #deliveryExists"() {
        setup:
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
        def indexInfo = new IndexInfo("my_delivery", "my_version", "my_collection", "my_index", "part0005", dataToSend.length, ChecksumType.MD5, "checksum-md5")
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
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "my_index"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "MD5"
            assert dis.readUTF() == "checksum-md5"
            def data = IOUtils.toByteArray(dis, dataToSend.length)
            dos.writeUTF(":success")
            dos.flush()
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
        }
        jis.importIndexFile(indexInfo, copyCallBackMock)
    }

    def "import index with invalid MD5 hash"() {
        when:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def indexInfo = new IndexInfo("my_delivery", "my_version", "my_collection", "my_index", "part0005", dataToSend.length, ChecksumType.MD5, "md5hash")
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
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "my_index"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "MD5"
            assert dis.readUTF() == "md5hash"
            def data = IOUtils.toByteArray(dis, dataToSend.length)
            dos.writeUTF(":error:checksum");
            dos.writeUTF("Wrong checksum");
            dos.flush()
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
            dos.flush()
        }
        jis.importIndexFile(indexInfo, copyCallBackMock)
        then:
        thrown JumboFileChecksumException
    }

    def "import data with valid MD5 hash"() {
        setup:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def dataInfo = new DataInfo("my_delivery", "my_version", "my_collection", "part0005", dataToSend.length, ChecksumType.MD5, "md5checksum")
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
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "MD5"
            assert dis.readUTF() == "md5checksum"
            def data = IOUtils.toByteArray(dis, dataToSend.length)
            dos.writeUTF(":success");
            dos.flush()
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
        }
        jis.importDataFile(dataInfo, copyCallBackMock)
        jis.getByteCount() == 195 // data length + meta data
    }

    def "import data with invalid MD5 hash"() {
        when:
        def dataToSendStr = "This is the binary test data! Usally it's binary and not readable, but for testing ok."
        def dataToSend = dataToSendStr.getBytes("UTF-8")
        def dataInfo = new DataInfo("my_delivery", "my_version", "my_collection", "part0005", dataToSend.length, ChecksumType.MD5, "md5checksum")
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
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readUTF() == "my_collection"
            assert dis.readUTF() == "part0005"
            assert dis.readLong() == dataToSend.length
            assert dis.readUTF() == "MD5"
            assert dis.readUTF() == "md5checksum"
            def data = IOUtils.toByteArray(dis, dataToSend.length)
            dos.writeUTF(":error:checksum");
            dos.writeUTF("error message");
            dos.flush()
            assert IOUtils.toString(data, "UTF-8") == dataToSendStr
        }
        jis.importDataFile(dataInfo, copyCallBackMock)
        then:
        thrown JumboFileChecksumException
    }

    def "initImport should be successful"() {
        expect:
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:init"
            assert dis.readUTF() == "delivery_key"
            assert dis.readUTF() == "version2"
            assert dis.readUTF() == "2012-12-12 12:12"
            assert dis.readUTF() == "some info"
            dos.writeUTF(":success")
            dos.flush()
        }
        jis.initImport(new ImportInfo("delivery_key", "version2", "2012-12-12 12:12", "some info"))
    }

    def "initImport should fail because version exists"() {
        when:
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:init"
            assert dis.readUTF() == "delivery_key"
            assert dis.readUTF() == "version2"
            assert dis.readUTF() == "2012-12-12 12:12"
            assert dis.readUTF() == "some info"
            dos.writeUTF(":error:deliveryversionexists")
            dos.writeUTF("my message")
            dos.flush()
        }
        jis.initImport(new ImportInfo("delivery_key", "version2", "2012-12-12 12:12", "some info"))
        then:
        thrown JumboDeliveryVersionExistsException
    }

    def "commitImport"() {
        expect:
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:commit"
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readBoolean()
            assert !dis.readBoolean()
            dos.writeUTF(":success")
            dos.flush()
        }
        jis.commitImport("my_delivery", "my_version", true, false)
    }

    def "handle wrong version"() {
        when:
        Thread.start {
            assert dis.readInt() == JumboConstants.IMPORT_PROTOCOL_VERSION
            assert dis.readUTF() == ":cmd:import:commit"
            assert dis.readUTF() == "my_delivery"
            assert dis.readUTF() == "my_version"
            assert dis.readBoolean()
            assert !dis.readBoolean()
            dos.writeUTF(":error:wrongversion")
            dos.writeUTF("my message")
            dos.flush()
        }
        jis.commitImport("my_delivery", "my_version", true, false)
        then:
        def ex = thrown JumboWrongVersionException
        ex.getMessage() == "my message"
    }
}
