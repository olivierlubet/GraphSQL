package graphsql

import graphsql.catalog.CatalogBuilder
import graphsql.controler.{FileLoader, SQLFileLoader}
import graphsql.graphx.{GraphBuilder, GraphWriter}
import org.apache.spark.sql.SparkSession

import util._

object Main extends App {

  println("GraphSQL")


  /**
    * SparkSession
    * def sql
    * -> parsePlan
    * https://github.com/apache/spark/blob/359375eff74630c9f0ea5a90ab7d45bf1b281ed0/sql/core/src/main/scala/org/apache/spark/sql/SparkSession.scala
    *
    * SparkSqlParser extends AbstractSqlParser
    * https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala
    *
    * AbstractSqlParser
    * def parsePlan
    * -> parse
    * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala
    *
    * def parse
    * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala
    */


  //val fileURL = getClass.getResource("/data.js")


  /*
  val folder = "C:\\Users\\hyma\\IdeaProjects\\GraphSQL\\src\\main\\resources\\sql"

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  getListOfFiles(folder).foreach(f => println(f.getName))
  */


  lazy val spark: SparkSession = SparkSession
    .builder
    .appName("GraphSQL")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  time {
    val catalog = new NFCatalog()
    val files = FileLoader.load(getClass.getResource("/input.conf"))
    println("Files to process:" + files.size)
/*
    files.foreach { f =>
      time {
        val url = getClass.getResource("/sql/" + f)
        println("Processing " + url.getFile)

        val sqls = SQLFileLoader.load(url)
        println(sqls.size + " more sql requests to process")

        sqls.foreach { sql =>
          val plan = Parser.parse(sql)
          CatalogBuilder(catalog).add(plan)
        }
        println("End of Processing " + url.getFile)
      }
    }
*/
    println("Building Graph")
    val g = GraphBuilder.buildFromCatalog(catalog)

    println("Writing Graph")
    val fileURL = getClass.getResource("/data.js")
    println(fileURL.getPath)
    GraphWriter.write(fileURL, g)
  }

  spark.close()
}

