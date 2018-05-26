package graphsql

import org.apache.spark.graphx.VertexId
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class NFCatalog {

  val databases: mutable.HashMap[String, NFDatabase] = mutable.HashMap.empty
  val unreferencedTables: ListBuffer[NFTable] = ListBuffer.empty
  val unreferencedColumns: ListBuffer[NFColumn] = ListBuffer.empty


  def getColumn
  (nameParts: Seq[String], scope: Seq[Vertex]): NFColumn = {
    nameParts.size match {
      case 1 =>
        val name = nameParts.head.toLowerCase
        if (scope.size > 1) {
          println("No table specification for column " + name)
          getColumn(name)
        } else {
          scope.head match {
            case c: NFColumn => c
            case table: NFTable => getColumn(name, table)
            case alias: NFTableAlias =>
              val a = getColumn(name, alias.table)
              val b = getColumn(name, alias.name, None)
              a.usedFor += b
              b
          }

        }
      case 2 =>
        val tableName = nameParts.head.toLowerCase
        scope.filter(p => p.name.toLowerCase == tableName)
           .head match {
          case c: NFColumn => c
          case table: NFTable => getColumn(nameParts(1), table)
          case alias: NFTableAlias =>
            val a = getColumn(nameParts(1), alias.table)
            val b = getColumn(nameParts(1), alias.name, None)
            a.usedFor += b
            b
        }
    }
  }

  def getColumn
  (columnName: String): NFColumn = {
    val columnNameL = columnName.toLowerCase
    val ret = new NFColumn(columnNameL)
    unreferencedColumns += ret
    ret
  }

  def getColumn
  (columnName: String, tableName: String, dbName: String): NFColumn =
    getColumn(columnName, getTable(tableName, dbName))

  def getColumn
  (columnName: String, tableName: String, dbName: Option[String]): NFColumn =
    getColumn(columnName, getTable(tableName, dbName))

  def getColumn
  (columnName: String, table: NFTable): NFColumn = {
    val columnNameL = columnName.toLowerCase

    table.columns.getOrElse(columnNameL, {
      val tmp = NFColumn(columnNameL, Some(table))
      table.columns += (columnNameL -> tmp)
      tmp
    })
  }

  def getTable(identifier: TableIdentifier): NFTable = {
    getTable(identifier.table, identifier.database)
  }

  def getTable
  (tableName: String, dbName: Option[String]): NFTable = dbName match {
    case None => getTable(tableName)
    case Some(s: String) => getTable(tableName, s)
  }

  def getTable(tableName: String): NFTable = {
    val tableNameL = tableName.toLowerCase
    val ret = new NFTable(tableNameL)
    unreferencedTables += ret
    ret
  }

  def getTable
  (tableName: String, dbName: String): NFTable = {
    val tableNameL = tableName.toLowerCase
    val dbNameL = dbName.toLowerCase

    val db: NFDatabase = databases.getOrElse(dbNameL, {
      val tmp: NFDatabase = NFDatabase(dbNameL)
      databases += (dbNameL -> tmp)
      tmp
    })

    db.tables.getOrElse(tableNameL, {
      val tmp: NFTable = new NFTable(tableNameL, db)
      db.tables += (tableNameL -> tmp)
      tmp
    })

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
  require(name == name.toLowerCase)

  val id: VertexId = Vertex.nextId

  val fullName: String

  val group = this.getClass.getSimpleName
}

case class NFTableAlias
(
  override val name: String,
  table: NFTable
) extends Vertex(name) {
  val fullName: String = name
}

/*
case class NFColumnAlias
(
  override val name: String,
  column: NFColumn
) extends Vertex(name) {

  val fullName: String = name
}
*/
case class NFColumn
(
  override val name: String,
  table: Option[NFTable] = None
) extends Vertex(name) {

  def this(name: String, table: NFTable) = this(name, Some(table))

  lazy val fullName: String = {
    table match {
      case Some(t: NFTable) => t.fullName + "."
      case None => ""
    }
  } + name

  val usedFor: ListBuffer[NFColumn] = ListBuffer.empty


}


case class NFTable
(
  override val name: String,
  database: Option[NFDatabase]
) extends Vertex(name) {

  def this(name: String) = this(name, None)

  def this(name: String, db: NFDatabase) = this(name, Some(db))

  lazy val fullName: String = {
    database match {
      case None => ""
      case Some(db: NFDatabase) => db.name + "."
    }
  } + name

  var columns: mutable.HashMap[String, NFColumn] = mutable.HashMap[String, NFColumn]()

}


case class NFDatabase
(
  override val name: String
) extends Vertex(name) {
  lazy val fullName: String = name
  val tables: mutable.HashMap[String, NFTable] = mutable.HashMap[String, NFTable]()

}
