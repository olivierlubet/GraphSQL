package graphsql.util

import java.io.File
import java.net.URL

import scala.io.Source
import scala.util.Try

object Control {
  def using[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close
    }

  def read(url: URL): Try[List[String]] = read(Source.fromURL(url, "UTF-8"))

  def read(file: File): Try[List[String]] = read(Source.fromFile(file))

  def read(source: Source): Try[List[String]] = {
    Try {
      val lines = using(source) { source =>
        (for (line <- source.getLines) yield line).toList
      }
      lines
    }
  }
}