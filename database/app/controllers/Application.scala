package controllers

import play.api._
import play.api.mvc._

import views._
import core._
import java.text.NumberFormat
import java.text.DateFormat
import models.ServerInformation
import java.io.File

object Application extends Controller {
  
  def index = Action {
    val runtime = Runtime.getRuntime()
    val format = NumberFormat.getInstance()
    val dateFormat = DateFormat.getDateTimeInstance
    val maxMemory = runtime.maxMemory()
    val allocatedMemory = runtime.totalMemory()
    val freeMemory = runtime.freeMemory()
    import play.api.Play.current
    val conf = Play.application.configuration
    val importPort = conf.getInt("jumbodb.import.port").get
    val queryPort = conf.getInt("jumbodb.query.port").get
    val dataPath = new File(conf.getString("jumbodb.datapath").get).getAbsolutePath
    val indexPath = new File(conf.getString("jumbodb.indexpath").get).getAbsolutePath
    val divideMB = 1024 * 1024

    val info = ServerInformation(
      queryPort = queryPort,
      importPort = importPort,
      dataPath = dataPath,
      indexPath = indexPath,
      maximumMemory = format.format(maxMemory / divideMB) + " MB",
      allocatedMemory = format.format(allocatedMemory / divideMB) + " MB",
      freeMemory = format.format(freeMemory / divideMB) + " MB",
      totalFreeMemory = format.format((freeMemory + (maxMemory - allocatedMemory)) / divideMB) + " MB",
      numberOfQueries = GlobalStatistics.getNumberOfQueries,
      numberOfResults = GlobalStatistics.getNumberOfResults,
      startupTime = dateFormat.format(GlobalStatistics.getStartupTime)
    )
    Ok(html.index(info))
  }
}