package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.ResultCallback
import org.jumbodb.database.service.query.snappy.SnappyStreamToFileCopy
import org.xerial.snappy.SnappyOutputStream

/**
 * @author Carsten Hufe
 */
class JsonSnappyRetrieveDataSetsTaskSpec extends spock.lang.Specification {

    def createTestData() {
        def data = ""
        for(i in 100000..101000) {
            data += '{msg: "This is a sample dataset", number: ' + i + '}\n'  // 50 chars long
        }
        data.getBytes("UTF-8")
    }

    def createTestFile() {
        def tmpFile = File.createTempFile("data", "file")
        def data = createTestData()
        SnappyStreamToFileCopy.copy(new ByteArrayInputStream(data), tmpFile, data.length, 32 * 1024)
        tmpFile
    }

    def "integrated test indexed search"() {
        setup:
        def file = createTestFile()
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyDataStrategy)
        dataStrategy.matches(_, _) >> true
        resultCallback.needsMore() >> true
        def offsets = [500l, 1000l, 1500l] as Set
        def jumboQuery = new JumboQuery()
        jumboQuery.addIndexQuery("testIndex", Arrays.asList(new QueryClause(QueryOperation.EQ, "somevalue")))
        def task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy)
        when:
        def numberOfResults = task.call()
        then:
        // implementation is a bit strange because internal spock cast from byte[] to Object[] fails
        3 * resultCallback.writeResult({ it ->
            def expectedResults = [
                    "{msg: \"This is a sample dataset\", number: 100010}",
                    "{msg: \"This is a sample dataset\", number: 100020}",
                    "{msg: \"This is a sample dataset\", number: 100030}"
            ]
            def foundDataSet = new String(it, "UTF-8")
            assert expectedResults.contains(foundDataSet)
            true
        })
        numberOfResults == 3
        cleanup:
        new File(file.getAbsolutePath() + ".chunks.snappy").delete()
        file.delete()
    }

    def "integrated test scanned search"() {
        // TODO
    }

    def "getResultBuffer calculate buffer size for data read"() {
       // TODO
    }

    def "concat to byte array buffers to one"() {
        // TODO
    }

    def "calculateChunkOffsetUncompressed"() {
        // TODO
    }

    def "calculateChunkOffsetCompressed"() {
        // TODO
    }

    def "matchingFilter"() {
        // TODO
    }

    def "getDataSetFromOffsetsGroup"() {
        // TODO
    }

    def "findDatasetLengthByLineBreak"() {
        // TODO
    }

    def "getBufferByOffsetGroup"() {
        // TODO
    }

    def "groupOffsetsByBufferSize"() {
        // TODO
    }
}
