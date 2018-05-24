package graphsql

import java.net.URL

import graphsql.controler.SQLFileLoader
import org.scalatest.FunSuite

import scala.io.Source

class SQLFileLoaderTest extends FunSuite {

  test ("eraseComment") {
    assertResult("test")(SQLFileLoader.eraseComment("test"))

    assertResult("test")(SQLFileLoader.eraseComment("test-- comm"))

    assertResult("test")(SQLFileLoader.eraseComment("test--\"rrr"))

    assertResult("test\"ti--ti")(SQLFileLoader.eraseComment("test\"ti--ti"))
    assertResult("test'ti--ti")(SQLFileLoader.eraseComment("test'ti--ti"))
    assertResult("test\"--\"")(SQLFileLoader.eraseComment("test\"--\"--ti--ti"))
  }
  test("testAddFile") {
    val sqls = SQLFileLoader.load(getClass.getResource("/test1.sql"))
    //println("sqls:")
    //sqls.foreach (println)
    assertResult(5)(sqls.length)
  }

}
