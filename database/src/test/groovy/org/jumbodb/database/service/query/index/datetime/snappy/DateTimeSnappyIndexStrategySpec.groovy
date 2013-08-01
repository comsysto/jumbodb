package org.jumbodb.database.service.query.index.datetime.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever


/**
 * @author Carsten Hufe
 */
class DateTimeSnappyIndexStrategySpec extends spock.lang.Specification {
    def strategy = new DateTimeSnappyIndexStrategy()

    def "verify strategy name"() {
        when:
        def strategyName = strategy.getStrategyName()
        then:
        strategyName == "DATETIME_SNAPPY_V1"
    }

    def "verify chunk size"() {
        when:
        def snappyChunkSize = strategy.getSnappyChunkSize()
        then:
        snappyChunkSize == 32640
    }

    def "readValueFromDataInput"() {
        setup:
        def disMock = Mock(DataInput)
        when:
        def value = strategy.readValueFromDataInput(disMock)
        then:
        1 * disMock.readLong() >> 1009l
        value == 1009l
    }

    def writeIndexEntry(value, dos) {
        dos.writeLong(value)
        dos.writeInt(123) // file hash
        dos.writeLong(567) // offset
    }

    def "readLastValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(123456789012345, dos)
        writeIndexEntry(123, dos)
        writeIndexEntry(345, dos)
        writeIndexEntry(543210987654321, dos)
        when:
        def value = strategy.readLastValue(byteArrayStream.toByteArray())
        then:
        value == 543210987654321
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "readFirstValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(123456789012345, dos)
        writeIndexEntry(123, dos)
        writeIndexEntry(345, dos)
        writeIndexEntry(543210987654321, dos)
        when:
        def value = strategy.readFirstValue(byteArrayStream.toByteArray())
        then:
        value == 123456789012345
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "createIndexFile"() {
        setup:
        def fileMock = Mock(File)
        when:
        def indexFile = strategy.createIndexFile(123, 456, fileMock)
        then:
        indexFile.getFrom() == 123
        indexFile.getTo() == 456
        indexFile.getIndexFile() == fileMock
    }

    def "findFileOffsets"() {
        // TODO

    }

    def "searchOffsetsByClauses"() {
        // TODO
    }

    def "isResponsibleFor"() {
        // TODO

    }

    def "buildIndexRanges"() {
        // TODO

    }

    def "createIndexFileDescription"() {
        // TODO

    }

    def "buildIndexRange"() {
        // TODO

    }

    def "onImport"() {
        // TODO

    }

    def "onInitialize"() {
        // TODO

    }


    def "onDataChanged"() {
        // TODO

    }


    def "acceptIndexFile operation"() {
        setup:
        def operationMock = Mock(DateTimeEqOperationSearch)
        def numberSnappyIndexFile = Mock(NumberSnappyIndexFile)
        def clause = new QueryClause(QueryOperation.EQ, "a date")
        def valueRetriever = Mock(QueryValueRetriever)
        def strategy = new DateTimeSnappyIndexStrategy()
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        when:
        def result = strategy.acceptIndexFile(clause, numberSnappyIndexFile)
        then:
        1 * operationMock.getQueryValueRetriever(clause) >> valueRetriever
        1 * operationMock.acceptIndexFile(valueRetriever, numberSnappyIndexFile) >> true
        result == true
        when:
        result = strategy.acceptIndexFile(clause, numberSnappyIndexFile)
        then:
        1 * operationMock.getQueryValueRetriever(clause) >> valueRetriever
        1 * operationMock.acceptIndexFile(valueRetriever, numberSnappyIndexFile) >> false
        result == false
    }


    def "acceptIndexFile exception"() {
        setup:
        def strategy = new DateTimeSnappyIndexStrategy() {
            @Override
            Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> getQueryOperationsStrategies() {
                return [:]
            }
        }
        when:
        strategy.acceptIndexFile(new QueryClause(QueryOperation.EQ, "a date"), Mock(NumberSnappyIndexFile))
        then:
        thrown UnsupportedOperationException
    }
}
