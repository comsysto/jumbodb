package models.deliveries

import org.apache.commons.io.FileUtils

/**
 * User: carsten
 * Date: 3/20/13
 * Time: 1:21 PM
 */
case class VersionedJumboCollection(
  collectionName: String,
  version: String,
  chunkKey: String,
  compressedSize: Long,
  uncompressedSize: Long,
  indexSize: Long,
  info: String,
  date: String,
  active: Boolean
) {
  val formatedCompressedSize = FileUtils.byteCountToDisplaySize(compressedSize)
  val formatedUncompressedSize = FileUtils.byteCountToDisplaySize(uncompressedSize)
  val formatedIndexSize = FileUtils.byteCountToDisplaySize(indexSize)
}
