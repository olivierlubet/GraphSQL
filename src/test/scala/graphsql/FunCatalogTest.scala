package graphsql

import org.scalatest.FunSuite

class FunCatalogTest extends FunSuite {

  test("generate") {
    val c = FunCatalog(List(new FunColumn("a")))

    assertResult(1)(c.columns.size)
  }
}
