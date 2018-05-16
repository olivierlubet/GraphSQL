package graphsql

import graphsql.catalog.CatalogBuilder
import graphsql.graphx.{GraphBuilder, GraphSQL}
import org.apache.spark.graphx.Graph
import org.scalatest.FunSuite

class GraphFromSqlTest extends FunSuite {


  def fromSqlToGraphX(sql: String): GraphSQL = {
    GraphBuilder.buildFromSql(sql)
  }

  def existOne(g: GraphSQL, columnName: String): Boolean = {
    1 == g.vertices.filter { case (_, v: Vertex) => v.fullName == columnName }.count
  }

  test("Big complex query") {
    val g = fromSqlToGraphX(
      """
        |  CREATE TABLE ${A}.foo AS
        |    SELECT
        |  CASE
        |  WHEN a.id_element IS NULL
        |  THEN b.id_element
        |  ELSE a.id_element
        |  END                                       AS id_element,
        |  CAST(a.mt_valeur_rachat AS DECIMAL(15,2)) AS mt_valeur_rachat,
        |  a.echeance_max,
        |  b.mt_premier_loyer,
        |  (b.tx_premier_loyer_base_locative/100) AS 	tx_premier_loyer_base_locative		-- transformer de pourcentage vers nombre décimal
        |    FROM
        |  (SELECT c.id_element AS id_element,
        |  c.echeance_max     AS echeance_max,
        |  d.mt_valeur_rachat AS mt_valeur_rachat
        |  FROM
        |  (SELECT id_element,
        |    MAX(no_echeance) AS echeance_max
        |      FROM ${BDD_COMMUN}.ekip_tabech
        |      WHERE code_statut='EXPL'
        |  GROUP BY id_element
        |  ) c
        |  INNER JOIN
        |    (SELECT id_element,
        |      mt_valeur_rachat,
        |      no_echeance
        |        FROM ${BDD_COMMUN}.ekip_tabech
        |        WHERE code_statut     ='EXPL'
        |  AND mt_valeur_rachat IS NOT NULL
        |  ) d
        |  ON (c.id_element  = d.id_element
        |    AND c.echeance_max=d.no_echeance)
        |  ) a
        |  FULL OUTER JOIN
        |  (SELECT e.id_element,
        |  t.no_echeance,
        |  t.interet                                AS interet,
        |  t.crb                                    AS crb,
        |  e.base_locative                          AS base_locative,
        |  CAST((t.interet +t.crb) AS                  DECIMAL(15,2)) AS mt_premier_loyer,
        |  CAST(((t.interet+t.CRB)/e.base_locative) AS DECIMAL(15,6)) AS tx_premier_loyer_base_locative,
        |  t.code_type_element,
        |  t.code_statut
        |  FROM ${BDD_COMMUN}.ekip_element e
        |    INNER JOIN ${BDD_COMMUN}.ekip_tabech t
        |    ON (e.id_element = t.id_element)
        |  WHERE ( t.no_echeance   = 1
        |    AND t.code_type_element = 'LOYE'
        |  AND t.ID_MVT           <> -1
        |  AND t.code_statut       ='EXPL')
        |  ) b ON ( a.id_element   = b.id_element )
      """.stripMargin)
    assert(existOne(g, "a.foo.mt_valeur_rachat"))
    assert(existOne(g, "a.foo.tx_premier_loyer_base_locative"))

  }

  test("Dates manipulations") {
    val g = fromSqlToGraphX(
      """
        |CREATE TABLE ${A}.foo AS
        |    SELECT
        |  MIN(date_eccheance_8601)                                                                               AS date_debut,
        |  MAX(date_eccheance_8601)                                                                               AS date_fin,
        |  months_between(MAX(date_eccheance_8601),MIN(date_eccheance_8601))+1                                    AS nb_mois,
        |  COUNT(DISTINCT date_echeance)                                                                          AS nb_echeances,
        |  COUNT(DISTINCT date_echeance)/(months_between(MAX(date_eccheance_8601),MIN(date_eccheance_8601))+1)*12 AS nb_echeances_par_an
        |  FROM ${BDD_COMMUN}.ekip_tabech
        |  WHERE code_statut='EXPL'
        |  GROUP BY id_element
      """.stripMargin)
    assert(existOne(g, "a.foo.date_debut"))
    assert(existOne(g, "a.foo.nb_echeances_par_an"))
  }


  test("Filter & Computations") {
    val g = fromSqlToGraphX(
      """
        | CREATE TABLE ${A}.foo AS
        |    SELECT DISTINCT id,
        |    CASE
        |      WHEN P='O'
        |      THEN (A*NUMERATEUR/DENOMINATEUR)
        |      ELSE B
        |      END AS mt
        |  FROM ${B}.bar
        |  WHERE C = 'B'
      """.stripMargin)
    assert(existOne(g, "a.foo.id"))
    assert(existOne(g, "a.foo.mt"))
  }

  test("CREATE + CAST") {
    val g = fromSqlToGraphX(
      """
        |CREATE TABLE ${A}.foo AS
        |SELECT concat('001_',a.id)           AS id,
        |  CAST(a.mt AS DECIMAL(15,2)) AS mt
        |  FROM ${B}.baz a
      """.stripMargin)
    assert(existOne(g, "a.foo.id"))
  }

  test("CREATE + IS NULL + IS NOT NULL") {
    val g = fromSqlToGraphX(
      """
        |CREATE TABLE ${A}.foo AS
        |SELECT
        |  CASE
        |        WHEN a.id3 IS  NOT NULL THEN a.id3
        |    WHEN a.id IS NULL THEN a.id2

        |    ELSE a.id
        |  END AS bar
        |FROM ${B}.baz a
      """.stripMargin)
    assert(existOne(g, "a.foo.bar"))
  }

  test("CREATE + MAX (and others functions)") {
    val g = fromSqlToGraphX(
      """CREATE TABLE ${B}.foo AS
        |SELECT a,
        |MAX(m) AS b
        |FROM ${A}.foo
        |GROUP BY a""".stripMargin)

    assert(existOne(g, "b.foo.b"))
    assert(existOne(g, "a.foo.a"))
  }
  test("MAX (and others functions)") {
    val g = fromSqlToGraphX(
      """
        |SELECT a,
        |  MAX(m) AS b
        |FROM ${A}.foo
        |GROUP BY a
      """.stripMargin)
    assert(existOne(g, "unknown.unknown.b"))
    assert(existOne(g, "a.foo.a"))
  }
  test("DISTINCT") {
    val g = fromSqlToGraphX(
      """
        |CREATE TABLE ${foo}.bar AS
        |SELECT DISTINCT a.baz AS id
        |FROM ${fo}.ba a
      """.stripMargin)
    assert(existOne(g, "foo.bar.id"))
  }

  test("STAR (A compléter)") {
    val g = fromSqlToGraphX(
      """
        |SELECT
        |  t1.*, -- commentaire
        |  t2.foo,
        |  t2.baz
        |FROM ${BDD}.bar t1
        |LEFT OUTER JOIN ${BDD}.foo t2
      """.stripMargin)

    assert(existOne(g, "bdd.bar.*"))
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
    assert(existOne(g, "unknown.t.foo"))
    assert(existOne(g, "unknown.unknown.baz"))
  }


  test("select foo as baz from t") {
    val sql = "select foo as baz from t"
    val g = fromSqlToGraphX(sql)
    assert(existOne(g, "unknown.unknown.baz"))
    assert(existOne(g, "unknown.t.foo"))

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

  test("select foo from ${EB}.t") {
    val sql = """select foo from ${EB}.t"""
    val g = fromSqlToGraphX(sql)
    assert(existOne(g, "eb.t.foo"))
  }

  test("create table baz as select id from foo") {
    val sql = "create table baz as select id from foo"
    val g = fromSqlToGraphX(sql)
    assert(existOne(g, "unknown.baz.id"))
    assert(existOne(g, "unknown.foo.id"))
    //g.vertices.foreach(println)
    //g.edges.foreach(println)
    assertResult(5)(g.edges.count())
    assertResult(1)(g.edges.filter(e => e.attr == "used for" && e.srcId < e.dstId).count())

  }

  test("select b.id from foo,baz b") {
    val sql = "select b.id from foo,baz b"
    val g = fromSqlToGraphX(sql)
    assert(existOne(g, "unknown.baz.id"))
  }

  test("select baz.id from foo,baz") {
    val sql = "select baz.id from foo,baz"
    val g = fromSqlToGraphX(sql)
    assert(existOne(g, "unknown.baz.id"))
  }

  test("select foo from baz") {
    val sql = "select foo from baz"
    val g = fromSqlToGraphX(sql)
    assert(existOne(g, "unknown.baz.foo"))
  }
}
