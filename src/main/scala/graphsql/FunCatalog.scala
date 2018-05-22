package graphsql

/**
  * Functionnal catalog
  *
  * @param scope
  * @param catalog
  */
case class FunCatalog(scope: List[Vertex], catalog: Option[FunCatalog] = None) {

  def this() = this(List.empty, None)

  def this(scope: List[Vertex]) = this(scope, None)

  lazy val columns: Set[FunColumn] = scope.flatMap {
    case c: FunColumn => Some(c)
  }.toSet ++ catalog match {
    case c: FunCatalog => c.columns
    case _ => Set.empty
  }

  def getColumn(columnName: String): FunColumn = {
    new FunColumn(columnName)
  }

  def getColumn(column: String, table: String, database: String): FunColumn = {
    val find: Iterable[FunColumn] = for {
      col: FunColumn <- columns
      if col.fullName == database + "." + table + "." + column
    } yield col

    if (find.nonEmpty) find.head
    else new FunColumn(column, Table(table, FunDatabase(database)))
  }

}


case class FunColumn
(
  override val name: String,
  table: Option[Table]
) extends Vertex(name) {

  def this(name: String) = this(name, None)

  def this(name: String, table: Table) = this(name, Some(table))

  lazy val fullName: String = tableName + name

  lazy val tableName: String = table match {
    case Some(t: Table) => t.fullName + "."
    case None => ""
  }

}

object Table {
}

case class Table
(
  override val name: String,
  database: FunDatabase
) extends Vertex(name) {

  lazy val fullName: String = database.name + "." + name

}

object FunDatabase {
}

case class FunDatabase
(
  override val name: String
) extends Vertex(name) {

  lazy val fullName: String = name

}
