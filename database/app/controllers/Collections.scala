package controllers

import play.api.mvc._
import java.io.File
import org.apache.commons.io.FileUtils
import com.typesafe.config.ConfigFactory
import java.io.FileInputStream
import java.io.DataInputStream
import play.api._

import views._

import models._
import models.collections._


object Collections extends Controller {
  import play.api.Play.current
  val conf = Play.application.configuration
  val indexPath = new File(conf.getString("jumbodb.indexpath").get)
  val dataPath = new File(conf.getString("jumbodb.datapath").get)


  def overview = Action {
    Ok(html.collections.overview(collections))
  }

  def collections = {
    val collectionFolders = dataPath.listFiles.filter(_.isDirectory).filter(!_.getName.startsWith("."))
    collectionFolders map { f =>
        val collectionName = f.getName
        JumboCollection(
          collapseId = "col" + f.getName.hashCode,
          name = collectionName,
          chunks = chunks(f, collectionName)
        )
    } sortBy(_.name)
  }

  def chunks(collectionFolder: File, collectionName: String) = {
    val chunkFolders = collectionFolder.listFiles.filter(_.isDirectory)
    chunkFolders map { f =>
      val activeProps = ConfigFactory.parseFile(new File(f.getAbsolutePath + "/active.properties"))
      val chunkKey = f.getName
        DeliveryChunk(
          key = chunkKey,
          versions = versions(f, collectionName, chunkKey, activeProps.getString("deliveryVersion"))
        )
    }
  }

  def versions(chunkFolder: File, collectionName: String, chunkKey: String, activeVersion: String) = {
    val versionFolders = chunkFolder.listFiles.filter(_.isDirectory)
    versionFolders map {  f =>
      val deliveryInfo = ConfigFactory.parseFile(new File(f.getAbsolutePath + "/delivery.properties"))
      val version = f.getName
      val compressedSize = FileUtils.sizeOfDirectory(f)
      val uncompressedSize = calculateUncompressed(f)
      val indexFolder = new File(indexPath.getAbsolutePath + "/" + collectionName + "/" + chunkKey + "/" + version)
      val indexSize = if(indexFolder.exists()) FileUtils.sizeOfDirectory(indexFolder) else 0

        DeliveryVersion(
          version = version,
          info = deliveryInfo.getString("info"),
          date = deliveryInfo.getString("date"),
          compressedSize = compressedSize,
          uncompressedSize = uncompressedSize,
          indexSize = indexSize,
          active = (activeVersion == version)
        )
    } sortWith(_.date > _.date)
  }

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