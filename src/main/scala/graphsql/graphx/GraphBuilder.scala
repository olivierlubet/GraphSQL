package graphsql.graphx

import java.net.URL

import graphsql.catalog.{CatalogBrowser, CatalogBuilder}
import graphsql.controler.FileLoader
import graphsql.{NFCatalog,  Parser, Vertex}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
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
    val sqls = FileLoader.load(url)
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
    val browser = new CatalogBrowser(catalog)
    val vertex = browser.vertices.map(v => (v.id, v))//v.fullName))
    val edges: Seq[Edge[String]] = browser.edges
    Graph(sc.parallelize(vertex), sc.parallelize(edges))
  }
}
