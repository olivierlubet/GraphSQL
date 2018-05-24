package graphsql.util

import java.net.URL
import scala.io.Source
import scala.util.Try

object Control {
  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    } finally {
      resource.close()
    }

  def readURL(url: URL): Try[List[String]] = {
    Try {
      val lines = using(Source.fromURL(url,"UTF-8")) { source =>
        (for (line <- source.getLines) yield line).toList
      }
      lines
    }
  }
}