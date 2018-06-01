package graphsql

import java.io.File

import graphsql.catalog.CatalogBuilder
import graphsql.controler._
import graphsql.graphx.{GraphBuilder, GraphWriter}
import org.apache.spark.sql.SparkSession
import util._

object Main extends App {

  case class Config
  (
    out: File = new File("C:\\Users\\hyma\\Documents\\GraphSQL\\data.js"),
    in: File = new File("C:\\Users\\hyma\\Documents\\GraphSQL\\")
  )

  val parser = new scopt.OptionParser[Config]("scopt") {
    head("GraphSQL", "0.1")

    opt[File]('i', "in").optional().valueName("<file>").
      text("Folder to scan for '.sql' files")
    opt[File]('o', "out").optional().valueName("<file>").
      text("Folder where GraphSQL will output its files")

    help("help").text("prints this usage text")
  }

  // parser.parse returns Option[C]
  parser.parse(args, Config()) match {
    case Some(config) => run(config)
    // do stuff

    case None =>
    // arguments are bad, error message will have been displayed
  }


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
  val folder = "C:\\Users\\hyma\\Documents\\GraphSQL\\sql"

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
  def run(config: Config): Unit = {
    lazy val spark: SparkSession = SparkSession
      .builder
      .appName("GraphSQL")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println(config)

    time {
      val catalog = new NFCatalog()

      val files = FileLoader.load(new File(config.in.getAbsolutePath+"/input.conf"))
      println("Files to process:" + files.size)

        files.foreach { f =>
          time {
            val file = new File(config.in.getAbsolutePath+"/sql/"+f)

            println("Processing " + file)

            val sqls = SQLFileLoader.load(file)
            println(sqls.size + " more sql requests to process")

            sqls.foreach { sql =>
              val plan = Parser.parse(sql)
              CatalogBuilder(catalog).add(plan)
            }
            println("End of Processing " + file)
          }
        }

      println("Building Graph")
      val g = GraphBuilder.buildFromCatalog(catalog)

      println("Writing Graph")

      println(config.out)
      GraphWriter.write(config.out, g)
    }

    spark.close()
  }
}

