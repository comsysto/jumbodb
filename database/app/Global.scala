import core.importer.ImportServer
import core.query.QueryServer
import play.api._
import java.io.File


/**
 * User: carsten
 * Date: 1/8/13
 * Time: 3:35 PM
 */
object Global extends GlobalSettings {

  var importServer: ImportServer = null
  var queryServer: QueryServer = null
//  var databaseQueryServer: DatabaseQueryServer = null

  override def onStart(app: Application) {
    val importPort = app.configuration.getInt("jumbodb.import.port").get
    val queryPort = app.configuration.getInt("jumbodb.query.port").get
    val dataPath = new File(app.configuration.getString("jumbodb.datapath").get)
    val indexPath = new File(app.configuration.getString("jumbodb.indexpath").get)

    Logger.info("Database Import Port: " + importPort)
    Logger.info("Database Query Port: " + queryPort)
    Logger.info("Data path: " + dataPath)
    Logger.info("Index path: " + indexPath)

    queryServer = new QueryServer(queryPort, dataPath, indexPath)
    importServer = new ImportServer(importPort, dataPath, indexPath, queryServer)
    queryServer.start()
    importServer.start()
  }

  override def onStop(app: Application) {
    importServer.stop()
    queryServer.stop()
  }

}
