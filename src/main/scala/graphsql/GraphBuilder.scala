package graphsql

import org.apache.spark.graphx

object GraphBuilder {
  val spark: SparkSession = SparkSession.builder.appName("GraphSQL").master("local").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val sc: SparkContext = spark.sparkContext

  def build(catalog: Catalog): Graph[String, String] = {

    val browser: Browser = new Browser(catalog)

    val vertex: Seq[(VertexId, String)] =
      browser.vertices.map(v => (v.id, v.fullName))

    val edges: Seq[Edge[String]] = browser.edges

    graphx.Graph(sc.parallelize(vertex), sc.parallelize(edges))
  }
}
