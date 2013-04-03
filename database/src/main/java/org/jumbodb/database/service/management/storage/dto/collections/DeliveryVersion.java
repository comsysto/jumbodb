package org.jumbodb.database.service.management.storage.dto.collections;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:13 PM
 */
public class DeliveryVersion {
    private String version;
    private String info;
    private String date;
    private long compressedSize;
    private long uncompressedSize;
    private long indexSize;
    private boolean active;
    // CARSTEN formating getter sollten reichen
    private String formatedCompressedSize;
    private String formatedUncompressedSize;
    private String formatedIndexSize;



//    version: String,
//    info: String,
//    date: String,
//    compressedSize: Long,
//    uncompressedSize: Long,
//    indexSize: Long,
//    active: Boolean
//    ) {
//        val formatedCompressedSize = FileUtils.byteCountToDisplaySize(compressedSize)
//        val formatedUncompressedSize = FileUtils.byteCountToDisplaySize(uncompressedSize)
//        val formatedIndexSize = FileUtils.byteCountToDisplaySize(indexSize)
}
