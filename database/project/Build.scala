import sbt._
import Keys._
import play.Project._

object ApplicationBuild extends Build {

    val appName         = "jumbodb"
    val appVersion      = "1.0-SNAPSHOT"

    val appDependencies = Seq(
      "org.webjars" % "jquery" % "1.9.1",
      "org.webjars" % "jquery-ui" % "1.9.2",
      "org.webjars" % "bootstrap" % "2.3.1",
      "org.webjars" % "angularjs" % "1.1.3",
      "commons-io" %  "commons-io"  % "2.4",
      "org.xerial.snappy" %  "snappy-java"  % "1.0.4.1",
      "net.minidev" %  "json-smart"  % "1.1.1",
      "com.google.guava" %  "guava"  % "13.0",
      "org.webjars" % "webjars-play" % "2.1.0"
  )

    val main = play.Project(appName, appVersion, appDependencies).settings(

    )
}
