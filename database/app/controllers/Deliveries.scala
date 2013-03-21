package controllers

import play.api.mvc._
import java.io.File
import org.apache.commons.io.FileUtils
import com.typesafe.config.ConfigFactory
import java.io.FileInputStream
import java.io.DataInputStream
import play.api._

import views._

import models.deliveries._


object Deliveries extends Controller {
  import play.api.Play.current
  val conf = Play.application.configuration
  val indexPath = new File(conf.getString("jumbodb.indexpath").get)
  val dataPath = new File(conf.getString("jumbodb.datapath").get)


  def overview = Action {
    Ok(html.deliveries.overview(chunkedDeliveries))
  }

  def chunkedDeliveries = {
    val collectionFolders = dataPath.listFiles.filter(_.isDirectory).filter(!_.getName.startsWith("."))
    val versionCollections = collectionFolders flatMap { collectionFolder =>
      val collectionName = collectionFolder.getName
      val deliveryChunkFolders = collectionFolder.listFiles.filter(_.isDirectory).filter(!_.getName.startsWith("."))
      deliveryChunkFolders flatMap { deliveryChunkFolder =>
        val chunkKey = deliveryChunkFolder.getName
        val versionFolders = deliveryChunkFolder.listFiles.filter(_.isDirectory).filter(!_.getName.startsWith("."))
        val activeProps = ConfigFactory.parseFile(new File(deliveryChunkFolder.getAbsolutePath + "/active.properties"))
        val activeVersion = activeProps.getString("deliveryVersion")
        versionFolders map { versionFolder =>
          val deliveryInfo = ConfigFactory.parseFile(new File(versionFolder.getAbsolutePath + "/delivery.properties"))
          val version = versionFolder.getName
          val compressedSize = FileUtils.sizeOfDirectory(versionFolder)
          val uncompressedSize = calculateUncompressed(versionFolder)
          val indexFolder = new File(indexPath.getAbsolutePath + "/" + collectionName + "/" + chunkKey + "/" + version)
          val indexSize = if(indexFolder.exists()) FileUtils.sizeOfDirectory(indexFolder) else 0
          VersionedJumboCollection(
            collectionName = collectionName,
            version = version,
            chunkKey = chunkKey,
            compressedSize = compressedSize,
            uncompressedSize = uncompressedSize,
            indexSize = indexSize,
            info = deliveryInfo.getString("info"),
            date = deliveryInfo.getString("date"),
            active = (activeVersion == version)
          )
        }
      }
    }

    val chunkKeyVersions = versionCollections.map(vc => (vc.chunkKey, vc.version)).distinct
    chunkKeyVersions map { case (chunkKey, version) =>
        val matchingCollections = versionCollections filter(vc => vc.version == version && vc.chunkKey == chunkKey) sortBy(_.collectionName)
        ChunkedDeliveryVersion(
          collapseId = "col" + chunkKey.hashCode + "-" + version.hashCode ,
          chunkKey = chunkKey,
          version = version,
          info = matchingCollections.map(_.info).distinct.mkString(" - "),
          date = matchingCollections.map(_.date).distinct.mkString(","),
          collections = matchingCollections
        )
    } sortWith(_.date > _.date)
  }

  //////////////

//  def chunkedDeliveries = {
//    val collectionFolders = dataPath.listFiles.filter(_.isDirectory).filter(!_.getName.startsWith("."))
//    collectionFolders map { f =>
//        val collectionName = f.getName
//        JumboCollection(
//          collapseId = "col" + f.getName.hashCode,
//          name = collectionName,
//          chunks = chunks(f, collectionName)
//        )
//    } sortBy(_.name)
//  }
//
//  def chunks(collectionFolder: File, collectionName: String) = {
//    val chunkFolders = collectionFolder.listFiles.filter(_.isDirectory)
//    chunkFolders map { f =>
//      val activeProps = ConfigFactory.parseFile(new File(f.getAbsolutePath + "/active.properties"))
//      val chunkKey = f.getName
//        DeliveryChunk(
//          key = chunkKey,
//          versions = versions(f, collectionName, chunkKey, activeProps.getString("deliveryVersion"))
//        )
//    }
//  }
//
//  def versions(chunkFolder: File, collectionName: String, chunkKey: String, activeVersion: String) = {
//    val versionFolders = chunkFolder.listFiles.filter(_.isDirectory)
//    versionFolders map {  f =>
//      val deliveryInfo = ConfigFactory.parseFile(new File(f.getAbsolutePath + "/delivery.properties"))
//      val version = f.getName
//      val compressedSize = FileUtils.sizeOfDirectory(f)
//      val uncompressedSize = calculateUncompressed(f)
//      val indexFolder = new File(indexPath.getAbsolutePath + "/" + collectionName + "/" + chunkKey + "/" + version)
//      val indexSize = if(indexFolder.exists()) FileUtils.sizeOfDirectory(indexFolder) else 0
//
//        DeliveryVersion(
//          version = version,
//          info = deliveryInfo.getString("info"),
//          date = deliveryInfo.getString("date"),
//          compressedSize = compressedSize,
//          uncompressedSize = uncompressedSize,
//          indexSize = indexSize,
//          active = (activeVersion == version)
//        )
//    } sortWith(_.date > _.date)
//  }

  def calculateUncompressed(versionsFolder: File) = {
    val metaFiles = versionsFolder.listFiles.filter(!_.isDirectory).filter(_.getName.endsWith(".chunks.snappy"))
    val metaSizes = metaFiles map { metaFile =>
      val chunksFis = new FileInputStream(metaFile)
      val chunksDis = new DataInputStream(chunksFis)
      val length = chunksDis.readLong()
      chunksDis.close
      chunksFis.close
      length
    }
    metaSizes.foldLeft(0L)(_ + _)
  }
}