package graphsql.catalog

import graphsql.{NFCatalog, NFColumn, NFDatabase, NFTable}

object CatalogStarExtender {
  def extend(catalog: NFCatalog): NFCatalog = {
    val stars = getStars(catalog)
    if (stars.nonEmpty) extend(catalog, stars)
    catalog
  }

  def getStars(catalog: NFCatalog): List[NFColumn] = {
    (
      catalog.databases.values.flatMap(getStars) ++
        catalog.unreferencedTables.flatMap(getStars) ++
        catalog.unreferencedColumns.filter(_.isStar)
      ).toList
  }

  def getStars(table: NFTable): List[NFColumn] =
    table.columns.values.filter(_.isStar).toList


  def getStars(database: NFDatabase): List[NFColumn] =
    database.tables.values.flatMap(getStars).toList

  def isExtendable(star: NFColumn): Boolean = star.comesFrom.count(_.isStar) == 0

  def extend(catalog: NFCatalog, list: List[NFColumn]): Unit = {
    val (ok, ko) = list.partition(isExtendable)
    ok.foreach(extend(catalog, _))

    if (ko.size == list.size) throw new Exception("No star can be resolved")
    if (ko.nonEmpty) extend(catalog, ko)
  }

  def extend(catalog: NFCatalog, star: NFColumn): Unit = {
    def linkToTable(starTo: NFColumn, tableFrom: NFTable): Unit = {
      for (cc <- tableFrom.columns.values if !cc.isStar) {
        val newCol = catalog.getColumn(cc.name, starTo.table)
        cc.linkTo(newCol)
      }
    }

    //println("extend " + star)
    //println(star.comesFrom)

    if (star.comesFrom.nonEmpty)
      throw new Exception("Should not exist")

    for (c <- star.usedFor if !c.isStar) {
      throw new Exception("Should not exist")
    }

    // Case * -> *
    for (c <- star.usedFor if c.isStar) star.table match { // on donne des antécédents à la seconde étoile
      case Some(t: NFTable) => linkToTable(c, t)
      case None => throw new Exception("Unimplemented")
    }


    // Cleanning the star

    star.usedFor.foreach { c =>
      c.unlinkFrom(star) // On casse les références entre source et *
    }

    star.table match {
      case Some(t: NFTable) =>
        t.columns.remove(star.name) // Ce faisant, on casse la dernière référence vers * (hors * -> * qui sera traité par la suite)
      case None => throw new Exception("Unimplemented")
    }
  }
}
