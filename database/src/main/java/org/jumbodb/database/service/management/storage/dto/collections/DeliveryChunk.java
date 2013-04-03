package org.jumbodb.database.service.management.storage.dto.collections;

import java.util.List;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:12 PM
 */
public class DeliveryChunk {
    private String key;
    private List<DeliveryVersion> versions;
    private long compressedSize;
    private long uncompressedSize;
    private long indexSize;
    // CARSTEN formating getter sollten reichen
    private String formatedCompressedSize;
    private String formatedUncompressedSize;
    private String formatedIndexSize;

//    key: String,
//    versions: Seq[DeliveryVersion]
//            ) {
//
//        val compressedSize = versions.map(_.compressedSize).foldLeft(0L)(_ + _)
//        val uncompressedSize = versions.map(_.uncompressedSize).foldLeft(0L)(_ + _)
//        val indexSize = versions.map(_.indexSize).foldLeft(0L)(_ + _)
//        val formatedCompressedSize = FileUtils.byteCountToDisplaySize(compressedSize)
//        val formatedUncompressedSize = FileUtils.byteCountToDisplaySize(uncompressedSize)
//        val formatedIndexSize = FileUtils.byteCountToDisplaySize(indexSize)
}
