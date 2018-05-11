package graphsql

import graphsql.catalog.CatalogBuilder
import graphsql.graphx.{GraphBuilder, GraphSQL}
import org.apache.spark.graphx.Graph
import org.scalatest.FunSuite

class GraphFromSqlTest extends FunSuite {
  /*
  test("projet de") {
    val sql ="""
        |CREATE TABLE ${BDD_LEASING_DATA_TMP}.projet_de AS
        |SELECT DISTINCT n_numero_dossier AS id_projet_origine,
        |  "204-DE"                       AS id_application,
        |  c_type_modele,
        |  c_pool              AS code_pool,
        |  d_edition_propo,
        |  p_pool_cal                AS partenaire_pool_cal,
        |  CASE f_demande_definitive
        |    WHEN 'O'
        |    THEN 1
        |    WHEN 'N'
        |    THEN 0
        |  END AS demande_definitive
        |FROM ${BDD_COMMUN}.de_dossiers
        |WHERE n_numero_dossier NOT IN ('null', '00')
      """.stripMargin
    val p = SqlParser.parse(sql)
    val g = GraphSql.buildGraphX(p)
    assertResult(1)(g.vertices.filter { case (_,c:Column) => c.name=="id_application" && c.table=="${BDD_LEASING_DATA_TMP}.projet_de" }.count)

  }*/

  def fromSqlToGraphX(sql: String): GraphSQL = {
    GraphBuilder.build(sql)
  }

  test("CASE WHEN THEN") {
    val sql =
      """
        |SELECT
        |  CASE foo
        |    WHEN 'O'
        |    THEN 1
        |    WHEN 'N'
        |    THEN 0
        |  END AS baz
        |FROM t
      """.stripMargin
    val g = fromSqlToGraphX(sql)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.t.foo" }.count)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.unknown.baz" }.count)
  }


  test("select foo as baz from t") {
    val sql = "select foo as baz from t"
    val g = fromSqlToGraphX(sql)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.t.foo" }.count)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.t.foo" }.count)

    //g.vertices.foreach(println)
    //g.edges.foreach(println)

    assertResult(3)(g.edges.count())
    assertResult(1)(g.edges.filter(e => e.attr == "used for" && e.srcId < e.dstId).count())
    //La colonne source a été créée avant la colonne de destination
  }

  test("drop table IF EXISTS baz") {
    val sql = "drop table IF EXISTS baz"
    val g = fromSqlToGraphX(sql)
    assertResult(0)(g.vertices.count)
  }

  test("select foo from ${ENV_DB}.t") {
    val sql = """select foo from ${ENV_DB}.t"""
    val g = fromSqlToGraphX(sql)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "ENV_DB.t.foo" }.count)
  }

  test("create table baz as select id from foo") {
    val sql = "create table baz as select id from foo"
    val g = fromSqlToGraphX(sql)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.baz.id" }.count)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.foo.id" }.count)
    //g.vertices.foreach(println)
    //g.edges.foreach(println)
    assertResult(5)(g.edges.count())
    assertResult(1)(g.edges.filter(e => e.attr == "used for" && e.srcId < e.dstId).count())

  }

  test("select b.id from foo,baz b") {
    val sql = "select b.id from foo,baz b"
    val g = fromSqlToGraphX(sql)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.baz.id" }.count)
  }

  test("select baz.id from foo,baz") {
    val sql = "select baz.id from foo,baz"
    val g = fromSqlToGraphX(sql)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.baz.id" }.count)
  }

  test("select foo from baz") {
    val sql = "select foo from baz"
    val g = fromSqlToGraphX(sql)
    assertResult(1)(g.vertices.filter { case (_, c: String) => c == "unknown.baz.foo" }.count)
  }
}
