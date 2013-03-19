package controllers

import play.api.mvc._
import play.api.data._
import play.api.data.Forms._

import views._

import models._

object Collections extends Controller {

  def overview = Action {
    Ok(html.collections.overview())
  }
}