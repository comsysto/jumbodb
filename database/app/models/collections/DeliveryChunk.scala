package models.collections

import org.apache.commons.io.FileUtils

/**
 * User: carsten
 * Date: 3/20/13
 * Time: 1:15 PM
 */
case class DeliveryChunk(
  key: String,
  versions: Seq[DeliveryVersion]
) {

  val compressedSize = versions.map(_.compressedSize).foldLeft(0L)(_ + _)
  val uncompressedSize = versions.map(_.uncompressedSize).foldLeft(0L)(_ + _)
  val indexSize = versions.map(_.indexSize).foldLeft(0L)(_ + _)
  val formatedCompressedSize = FileUtils.byteCountToDisplaySize(compressedSize)
  val formatedUncompressedSize = FileUtils.byteCountToDisplaySize(uncompressedSize)
  val formatedIndexSize = FileUtils.byteCountToDisplaySize(indexSize)
}