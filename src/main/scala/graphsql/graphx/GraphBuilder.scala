package graphsql.graphx

import java.net.URL

import graphsql.catalog.{CatalogBrowser, CatalogBuilder}
import graphsql.controler.FileLoader
import graphsql.{Catalog, Parser}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object GraphBuilder {
  val spark: SparkSession = SparkSession
    .builder
    .appName("GraphSQL")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val sc: SparkContext = spark.sparkContext

  def buildFromURL(url: URL, catalog: Catalog = new Catalog)
  : GraphSQL = {
    val sqls = FileLoader.load(url)
    sqls.foreach { sql =>
      val plan = Parser.parse(sql)
      CatalogBuilder(catalog).add(plan)
    }
    GraphBuilder.buildFromCatalog(catalog)
  }

  def buildFromSql(sql: String, catalog: Catalog = new Catalog)
  : GraphSQL = {
    val plan = Parser.parse(sql)
    CatalogBuilder(catalog).add(plan)
    GraphBuilder.buildFromCatalog(catalog)
  }

  def buildFromCatalog(catalog: Catalog): GraphSQL = {
    val browser: CatalogBrowser = new CatalogBrowser(catalog)
    val vertex: Seq[(VertexId, String)] =
      browser.vertices.map(v => (v.id, v.fullName))
    val edges: Seq[Edge[String]] = browser.edges
    val a: RDD[(VertexId, String)] = sc.parallelize(vertex)
    val b: RDD[Edge[String]] = sc.parallelize(edges)
    Graph(sc.parallelize(vertex), sc.parallelize(edges))
  }
}
