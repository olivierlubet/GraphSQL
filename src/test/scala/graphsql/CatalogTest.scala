package graphsql

class CatalogTest extends FunSuite {


  test("test retrieve") {
    val catalog = new Catalog
    val c1 = catalog.getColumn("col", "tab", "db")
    val c2 = catalog.getColumn("col", "tab", "db")
    assert(c1.id == c2.id)
  }

  test("test other") {
    val catalog = new Catalog
    val c1 = catalog.getColumn("col", "foo", "db")
    val c2 = catalog.getColumn("col", "baz", "db")
    assert(c1.id != c2.id)
  }

}
