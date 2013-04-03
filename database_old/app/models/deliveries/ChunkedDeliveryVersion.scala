package models.deliveries

import org.apache.commons.io.FileUtils

/**
 * User: carsten
 * Date: 3/20/13
 * Time: 1:12 PM
 */
case class ChunkedDeliveryVersion(
  collapseId: String,
  chunkKey: String,
  version: String,
  info: String,
  date: String,
  collections: Seq[VersionedJumboCollection]
) {
    val compressedSize = collections.map(_.compressedSize).foldLeft(0L)(_ + _)
    val uncompressedSize = collections.map(_.uncompressedSize).foldLeft(0L)(_ + _)
    val indexSize = collections.map(_.indexSize).foldLeft(0L)(_ + _)
    val formatedCompressedSize = FileUtils.byteCountToDisplaySize(compressedSize)
    val formatedUncompressedSize = FileUtils.byteCountToDisplaySize(uncompressedSize)
    val formatedIndexSize = FileUtils.byteCountToDisplaySize(indexSize)
    val allRunning = collections.forall(_.active)
    val noneRunning = collections.forall(!_.active)
    val someRunning = !allRunning && !noneRunning
}
