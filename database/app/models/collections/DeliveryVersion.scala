package models.collections

import org.apache.commons.io.FileUtils

/**
 * User: carsten
 * Date: 3/20/13
 * Time: 1:12 PM
 */
case class DeliveryVersion(
  version: String,
  info: String,
  date: String,
  compressedSize: Long,
  uncompressedSize: Long,
  indexSize: Long,
  active: Boolean
) {
  val formatedCompressedSize = FileUtils.byteCountToDisplaySize(compressedSize)
  val formatedUncompressedSize = FileUtils.byteCountToDisplaySize(uncompressedSize)
  val formatedIndexSize = FileUtils.byteCountToDisplaySize(indexSize)
}
