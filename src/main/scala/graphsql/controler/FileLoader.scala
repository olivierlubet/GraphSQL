package graphsql.controler

import java.io.File
import java.net.URL

import graphsql.util.Control

import scala.util.{Failure, Success, Try}

object FileLoader {
  def load(url: URL): List[String] = {
    Control.read(url) match {
      case Failure(s) =>
        println(s"Failed reading $url : $s")
        List()
      case Success(lines) => lines
    }
  }

  def load(file:File):List[String] = load(file.toURI.toURL)
}
