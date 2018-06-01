package graphsql.graphx

import java.net.URL

import graphsql.catalog._
import graphsql.controler._
import graphsql._
import org.apache.spark.graphx._

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object GraphBuilder {
  lazy val spark: SparkSession = SparkSession
    .builder
    .appName("GraphSQL")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  lazy val sc: SparkContext = spark.sparkContext


  def buildFromURL(url: URL, catalog: NFCatalog = new NFCatalog)
  : GraphSQL = {
    val sqls = SQLFileLoader.load(url)
    sqls.foreach { sql =>
      val plan = Parser.parse(sql)
      CatalogBuilder(catalog).add(plan)
    }
    GraphBuilder.buildFromCatalog(catalog)
  }

  def buildFromSql(sql: String, catalog: NFCatalog = new NFCatalog)
  : GraphSQL = {
    val plan = Parser.parse(sql)
    CatalogBuilder(catalog).add(plan)
    GraphBuilder.buildFromCatalog(catalog)
  }

  def buildFromCatalog(catalog: NFCatalog): GraphSQL = {
    val catalogExt = CatalogStarExtender.extend(catalog)
    val browser = new CatalogBrowser(catalogExt)
    val vertex = browser.vertices.map(v => (v.id, v))
    val edges: Seq[Edge[String]] = browser.edges
    Graph(sc.parallelize(vertex), sc.parallelize(edges))
  }
}
