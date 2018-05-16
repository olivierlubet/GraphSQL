package graphsql

import org.apache.spark.graphx.VertexId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class Catalog {


  var databases: mutable.HashMap[String, Database] = mutable.HashMap.empty

  def getColumn
  (columnName: String, tableName: String, dbName: String): Column = {
    val columnNameL=columnName.toLowerCase
    val tableNameL=tableName.toLowerCase
    val dbNameL=dbName.toLowerCase

    if (tableName == "select") { // cas d'une table temporaire liée à un SELECT
      Column(columnNameL, None)
    } else {
      val db: Database = databases.getOrElse(dbNameL, {
        val tmp: Database = Database(dbNameL)
        databases += (dbNameL -> tmp)
        tmp
      })

      val table: Table = db.tables.getOrElse(tableNameL, {
        val tmp: Table = Table(tableNameL, db)
        db.tables += (tableNameL -> tmp)
        tmp
      })

      val column: Column = table.columns.getOrElse(columnNameL, {
        val tmp = Column(columnNameL, Some(table))
        table.columns += (columnNameL -> tmp)
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

abstract class Vertex (val name:String) extends Serializable {

  val id: VertexId = Vertex.nextId

  val fullName: String
  Vertex.innerCatalog += this

  val group = this.getClass.getSimpleName
}

case class Column
(
  override val name: String,
  table: Option[Table]
) extends Vertex (name){
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
  override val name: String,
  database: Database
) extends Vertex (name){

  lazy val fullName: String = database.name + "." + name

  var columns: Map[String, Column] = Map[String, Column]()

}

case class Database
(
  override val name: String
) extends Vertex (name){
  lazy val fullName: String = name
  var tables: Map[String, Table] = Map[String, Table]()

}
