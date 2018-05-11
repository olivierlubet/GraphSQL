package graphsql

import graphsql.graphx.GraphBuilder
import org.scalatest.FunSuite

class GraphFromUrlTest extends FunSuite {
  test("test1.sql") {
    val g = GraphBuilder.build(getClass.getResource("/test1.sql"))

    //g.triplets.foreach{e =>
    //  println(s"$e.srcAttr $e.attr $e.dstAttr")
    //}
    assertResult(1)(g.triplets.filter { t =>
      t.srcAttr == "BDD_LEASING_DATA_TMP.tiers_bt_corres_crca.c_code_crca_maitre" &&
        t.dstAttr == "BDD_LEASING_DATA_TMP.tiers_tmp.c_code_crca_maitre" &&
        t.attr == "used for"
    }.count)
  }
}
