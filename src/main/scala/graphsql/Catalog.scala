package graphsql

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class Catalog {


  var databases: mutable.HashMap[String, Database] = mutable.HashMap.empty

  def getColumn
  (columnName: String, tableName: String, dbName: String): Column = {
    if (tableName == "select") { // cas d'une table temporaire liée à un SELECT
      Column(columnName, None)
    } else {
      val db: Database = databases.getOrElse(dbName, {
        val tmp: Database = Database(dbName)
        databases += (dbName -> tmp)
        tmp
      })

      val table: Table = db.tables.getOrElse(tableName, {
        val tmp: Table = Table(tableName, db)
        db.tables += (tableName -> tmp)
        tmp
      })

      val column: Column = table.columns.getOrElse(columnName, {
        val tmp = Column(columnName, Some(table))
        table.columns += (columnName -> tmp)
        tmp
      })

      column
    }
  }
}


object Vertex {
  private var counter: VertexId = 0
  private var innerCatalog: ListBuffer[Vertex] = ListBuffer.empty


  def nextId: VertexId = {
    counter += 1
    counter
  }

  def getAll: Seq[Vertex] = {
    innerCatalog
  }
}

abstract class Vertex {

  val id: VertexId = Vertex.nextId

  val fullName: String

  Vertex.innerCatalog += this
}

case class Column
(
  name: String,
  table: Option[Table]
) extends Vertex {
  lazy val fullName: String = databaseName + "." + tableName + "." + name
  lazy val tableName: String = table match {
    case Some(t: Table) => t.name
    case None => "unknown"
  }
  lazy val databaseName: String = table match {
    case Some(t: Table) => t.database.name
    case None => "unknown"
  }
  val usedFor: ListBuffer[Column] = ListBuffer.empty

  def this(name: String, table: Table) = this(name, Some(table))


}

case class Table
(
  name: String,
  database: Database
) extends Vertex {

  lazy val fullName: String = database.name + "." + name

  var columns: Map[String, Column] = Map[String, Column]()

}

case class Database
(
  name: String
) extends Vertex {
  lazy val fullName: String = name
  var tables: Map[String, Table] = Map[String, Table]()

}
