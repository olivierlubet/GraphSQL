package graphsql

import java.net.URL

import graphsql.catalog.{CatalogBrowser, CatalogBuilder}
import graphsql.controler.FileLoader
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.sql.SparkSession

object GraphBuilder {
  val spark: SparkSession = SparkSession
    .builder
    .appName("GraphSQL")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val sc: SparkContext = spark.sparkContext

  def buildFromURL(url: URL, catalog: Catalog = new Catalog)
  : Graph[String, String] = {
    val sqls = FileLoader.load(url)
    sqls.foreach { sql =>
      val plan = Parser.parse(sql)
      CatalogBuilder(catalog).add(plan)
    }
    GraphBuilder.buildFromCatalog(catalog)
  }

  def buildFromSql(sql: String, catalog: Catalog = new Catalog)
  : Graph[String, String] = {
    val plan = Parser.parse(sql)
    CatalogBuilder(catalog).add(plan)
    GraphBuilder.buildFromCatalog(catalog)
  }

  def buildFromCatalog(catalog: Catalog): Graph[String, String] = {
    val browser: CatalogBrowser = new CatalogBrowser(catalog)
    val vertex: Seq[(VertexId, String)] =
      browser.vertices.map(v => (v.id, v.fullName))
    val edges: Seq[Edge[String]] = browser.edges
    graphx.Graph(sc.parallelize(vertex), sc.parallelize(edges))
  }
}
