package controllers

import play.api._
import play.api.mvc._

import views._

object Application extends Controller {
  
  def index = Action {
    Ok(html.index())
  }

  // CARSTEN könnte als alternativer upload weg bestehen bleiben
  def upload = Action(parse.multipartFormData) { request =>
    request.body.file("part-file").map { picture =>
      import java.io.File
      val filename = picture.filename
      val contentType = picture.contentType
      picture.ref.moveTo(new File("/Users/carsten/myfile"))
      println("File uploaded")
      Ok("File uploaded")
    }.getOrElse {
      Redirect(routes.Application.index).flashing(
        "error" -> "Missing file"
      )
    }
  }
  
}