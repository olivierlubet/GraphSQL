package graphsql

import graphsql.graphx.GraphBuilder
import org.apache.spark.graphx.{EdgeTriplet, VertexId}
import org.scalatest.FunSuite

class GraphFromUrlTest extends FunSuite {
  test("test1.sql") {
    val g = GraphBuilder.buildFromURL(getClass.getResource("/test1.sql"))

    g.triplets.foreach{case e:EdgeTriplet[String,String] =>
      val (src,attr,dst) = (e.srcAttr,e.attr,e.dstAttr)
      println(src+" "+attr+" "+dst)
    }
    assertResult(1)(g.triplets.filter { t =>
      t.srcAttr == "BDD_LEASING_DATA_TMP.tiers_bt_corres_crca.c_code_crca_maitre" &&
        t.dstAttr == "BDD_LEASING_DATA_TMP.tiers_tmp.c_code_crca_maitre" &&
        t.attr == "used for"
    }.count)
  }
}
