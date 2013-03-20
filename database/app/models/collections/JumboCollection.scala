package models.collections

import org.apache.commons.io.FileUtils
/**
 * User: carsten
 * Date: 3/20/13
 * Time: 1:21 PM
 */
case class JumboCollection(
  collapseId: String,
  name: String,
  chunks: Seq[DeliveryChunk]
) {

  val compressedSize = chunks.map(_.compressedSize).foldLeft(0L)(_ + _)
  val uncompressedSize = chunks.map(_.uncompressedSize).foldLeft(0L)(_ + _)
  val indexSize = chunks.map(_.indexSize).foldLeft(0L)(_ + _)
  val formatedCompressedSize = FileUtils.byteCountToDisplaySize(compressedSize)
  val formatedUncompressedSize = FileUtils.byteCountToDisplaySize(uncompressedSize)
  val formatedIndexSize = FileUtils.byteCountToDisplaySize(indexSize)
}
