package graphsql

object Main extends App {

  println("Hello")


  /**
    * SparkSession
    * def sql
    * -> parsePlan
    * https://github.com/apache/spark/blob/359375eff74630c9f0ea5a90ab7d45bf1b281ed0/sql/core/src/main/scala/org/apache/spark/sql/SparkSession.scala
    *
    * SparkSqlParser extends AbstractSqlParser
    * https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala
    *
    * AbstractSqlParser
    * def parsePlan
    * -> parse
    * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala
    *
    * def parse
    * https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ParseDriver.scala
    */

  val sql = "select b.id i, titi, 'toto' as c from baz b,foo"
  val p = Parser.parse(sql)
}

