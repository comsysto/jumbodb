package org.jumbodb.database.service.management.status

import com.google.common.io.Files
import org.apache.commons.lang.StringUtils
import org.jumbodb.database.service.configuration.JumboConfiguration
import org.jumbodb.database.service.importer.DatabaseImportSession
import org.jumbodb.database.service.management.status.dto.ServerInformation
import org.jumbodb.database.service.query.DatabaseQuerySession
import org.jumbodb.database.service.statistics.GlobalStatistics
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class StatusServiceSpec extends Specification {
    def "get some status server information"() {
        setup:
        def dataDir = Files.createTempDir()
        def indexDir = Files.createTempDir()
        def jumboConfig = new JumboConfiguration(12002, 12001, dataDir, indexDir)
        def statusService = new StatusService(jumboConfig)
        GlobalStatistics.incNumberOfQueries(100)
        GlobalStatistics.incNumberOfResults(5000)
        when:
        def info = statusService.getStatus()
        then:
        info.getQueryPort() == 12002
        info.getImportPort() == 12001
        info.getDataPath() == dataDir.getAbsolutePath()
        info.getIndexPath() == indexDir.getAbsolutePath()
        info.getQueryProtocolVersion() == DatabaseQuerySession.PROTOCOL_VERSION.toString()
        info.getImportProtocolVersion() == DatabaseImportSession.PROTOCOL_VERSION.toString()
        info.getNumberOfQueries() > 0l
        info.getNumberOfResults() == 5000l
        StringUtils.isNotBlank(info.getAllocatedMemory())
        StringUtils.isNotBlank(info.getMaximumMemory())
        StringUtils.isNotBlank(info.getFreeMemory())
        StringUtils.isNotBlank(info.getAllocatedMemory())
        StringUtils.isNotBlank(info.getStartupTime())
        StringUtils.isNotBlank(info.getStorageFormatVersion())
        StringUtils.isNotBlank(info.getIndexDiskFreeSpace())
        StringUtils.isNotBlank(info.getIndexDiskTotalSpace())
        StringUtils.isNotBlank(info.getIndexDiskUsedSpace())
        info.getIndexDiskUsedSpacePerc() != null
        StringUtils.isNotBlank(info.getDataDiskFreeSpace())
        StringUtils.isNotBlank(info.getDataDiskTotalSpace())
        StringUtils.isNotBlank(info.getDataDiskUsedSpace())
        info.getDataDiskUsedSpacePerc() != null
        cleanup:
        dataDir.delete()
        indexDir.delete()
    }
}
