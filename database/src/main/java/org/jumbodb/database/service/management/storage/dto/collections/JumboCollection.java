package org.jumbodb.database.service.management.storage.dto.collections;

import java.util.List;

/**
 * User: carsten
 * Date: 4/3/13
 * Time: 8:12 PM
 */
public class JumboCollection {
    private String collapseId;
    private String name;
    private List<DeliveryChunk> chunks;
    private long compressedSize;
    private long uncompressedSize;
    private long indexSize;
    // CARSTEN formating getter sollten reichen
    private String formatedCompressedSize;
    private String formatedUncompressedSize;
    private String formatedIndexSize;
//    collapseId: String,
//    name: String,
//    chunks: Seq[DeliveryChunk]
//            ) {
//
//        val compressedSize = chunks.map(_.compressedSize).foldLeft(0L)(_ + _)
//        val uncompressedSize = chunks.map(_.uncompressedSize).foldLeft(0L)(_ + _)
//        val indexSize = chunks.map(_.indexSize).foldLeft(0L)(_ + _)
//        val formatedCompressedSize = FileUtils.byteCountToDisplaySize(compressedSize)
//        val formatedUncompressedSize = FileUtils.byteCountToDisplaySize(uncompressedSize)
//        val formatedIndexSize = FileUtils.byteCountToDisplaySize(indexSize)
}
