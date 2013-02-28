import sbt._
import Keys._
import PlayProject._

object ApplicationBuild extends Build {

    val appName         = "jumbodb"
    val appVersion      = "1.0-SNAPSHOT"

    val appDependencies = Seq(
      "commons-io" %  "commons-io"  % "2.4",
      "com.jquery" %  "jquery"  % "1.7.2-1",
      "com.jqueryui" %  "jquery.ui"  % "1.8.20",
      "org.angularjs" %  "angular"  % "1.0.1-1",
      "org.xerial.snappy" %  "snappy-java"  % "1.0.4.1",
      "com.github.twitter" %  "bootstrap"  % "2.1.0",
      "net.minidev" %  "json-smart"  % "1.1.1"

    )

    val main = PlayProject(appName, appVersion, appDependencies, mainLang = SCALA).settings(
    )

}
