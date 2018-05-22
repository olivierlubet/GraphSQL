package graphsql

import org.apache.spark.graphx.VertexId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class NFCatalog {
  val databases: mutable.HashMap[String, NFDatabase] = mutable.HashMap.empty

  def getColumn
  (columnName: String): NFColumn = {
    val columnNameL = columnName.toLowerCase
    new NFColumn(columnNameL)
  }

  def getColumn
  (columnName: String, tableName: String, dbName: String): NFColumn = {
    val columnNameL = columnName.toLowerCase
    val tableNameL = tableName.toLowerCase
    val dbNameL = dbName.toLowerCase

    val db: NFDatabase = databases.getOrElse(dbNameL, {
      val tmp: NFDatabase = NFDatabase(dbNameL)
      databases += (dbNameL -> tmp)
      tmp
    })

    val table: NFTable = db.tables.getOrElse(tableNameL, {
      val tmp: NFTable = NFTable(tableNameL, db)
      db.tables += (tableNameL -> tmp)
      tmp
    })

    val column: NFColumn = table.columns.getOrElse(columnNameL, {
      val tmp = NFColumn(columnNameL, Some(table))
      table.columns += (columnNameL -> tmp)
      tmp
    })

    column
  }
}


object Vertex {
  private var counter: VertexId = 0

  def nextId: VertexId = {
    counter += 1
    counter
  }

}

abstract class Vertex(val name: String) extends Serializable {

  val id: VertexId = Vertex.nextId

  val fullName: String

  val group = this.getClass.getSimpleName
}

case class NFColumn
(
  override val name: String,
  table: Option[NFTable] = None
) extends Vertex(name) {

  lazy val fullName: String = tableName + name
  lazy val tableName: String = table match {
    case Some(t: NFTable) => t.fullName + "."
    case None => ""
  }

  val usedFor: ListBuffer[NFColumn] = ListBuffer.empty

  def this(name: String, table: NFTable) = this(name, Some(table))

}

object NFTable {
  //val UNKNOWN = "unknown"
  //val SELECT = "select"
  //val UNION = "union"
}

case class NFTable
(
  override val name: String,
  database: NFDatabase
) extends Vertex(name) {

  lazy val fullName: String = database.name + "." + name

  var columns: mutable.HashMap[String, NFColumn] = mutable.HashMap[String, NFColumn]()

}

object NFDatabase {
  //val TEMPORARY = "tmp"
}

case class NFDatabase
(
  override val name: String
) extends Vertex(name) {
  lazy val fullName: String = name
  val tables: mutable.HashMap[String, NFTable] = mutable.HashMap[String, NFTable]()

}
