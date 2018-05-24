package graphsql.controler

import java.net.URL

import graphsql.util.Control

import scala.util.{Failure, Success}

object FileLoader {

  def load(url: URL): List[String] = {

    Control.readURL(url) match {
      case Failure(s) =>
        println(s"Failed reading $url : $s")
        List()
      case Success(lines) => lines
    }
  }

}
