package graphsql.catalog

import graphsql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.types.StructField


case class CatalogBuilder(catalog: NFCatalog = new NFCatalog) {


  // TODO: réécrire le code de façon fonctionnelle (à commencer par le catalogue)

  def add(plan: LogicalPlan): NFCatalog = {
    //println(plan)
    buildFromPlan(plan)
    catalog
  }

  ////////////////////// LOGICAL PLANS ////////////////////////

  private def buildFromPlan(plan: LogicalPlan): Seq[Vertex] = plan match {
    /* UNARY NODES */
    case p: Project => // SELECT
      val scope = buildFromPlan(p.child)
      buildFromExpressions(scope, p.projectList)

    case d: Distinct =>
      buildFromPlan(d.child)

    case a: Aggregate =>
      val scope = buildFromPlan(a.child)
      buildFromExpressions(scope, a.aggregateExpressions)

    case sa: SubqueryAlias =>
      val childs = buildFromPlan(sa.child)

      val table = catalog.getTable(sa.alias)

      val ret = childs.flatMap{
        case t: NFTable => // FROM a AS b
          Option(NFTableAlias(sa.alias.toLowerCase, t))
        case c: NFColumn => // (SELECT a,z FROM b) AS c || SELECT a,z FROM b UNION SELECT x,y FROM d
          val ret = catalog.getColumn(c.name, table)
          c.usedFor += ret
          None // Les colonnes sont empaquetées dans la table
      }

      // S'il existe au moins une colonne, on retourne la table du contexte en scope
      if (childs.exists {
        case _: NFColumn => true
        case _ => false
      }) ret :+ table
      else ret


    // Autres Unary Nodes
    case _: Filter | _: Aggregate | _: Distinct | _: Sort =>
      buildFromPlan(plan.asInstanceOf[UnaryNode].child)

    /* PLAN with subqueries */
    case u: Union => buildFromPlan(u)
    case c: CreateTable => buildFromPlan(c)
    case i: InsertIntoTable => buildFromPlan(i)
    case c: CreateTableLikeCommand => buildFromPlan(c)

    /* BINARY NODES */
    case j: Join => // JOIN
      buildFromPlan(j.left) ++ buildFromPlan(j.right)

    /* LEAF */
    case r: UnresolvedRelation =>
      Seq(catalog.getTable(r.tableIdentifier))

    /* RUNNABLE COMMAND */
    // Nothing to do
    case _: DropTableCommand |
         _: AlterTableRenameCommand | // TODO : gérer correctement le AlterTable // eg: tiers -> tiers_tmp (note : ne vaut que pour AIR)
         _: SetCommand | // eg: "hive.exec.parallel=true"
         _: CreateDatabaseCommand => // eg: "CREATE DATABASE IF NOT EXISTS"
      Seq.empty

    case _ => throw new Exception("Unimplemented:\n" + plan)
  }

  // Note : on crée des colonnes identiques, mais on ne les lies pas !
  private def buildFromPlan(c: CreateTableLikeCommand): Seq[Vertex] = {
    val target = catalog.getTable(c.targetTable)
    val source = catalog.getTable(c.sourceTable)
    source.columns.values.map {
      col: NFColumn =>
        catalog.getColumn(col.name, target)
    }.toSeq
  }

  private def buildFromPlan(i: InsertIntoTable): Seq[Vertex] = {
    val table = buildFromPlan(i.table).head.asInstanceOf[NFTable] // Bug potentiel ?
    buildFromPlan(i.query).map {
      case c: NFColumn =>
        val newCol = catalog.getColumn(c.name, table)
        c.usedFor += newCol
        newCol
    }
  }

  private def buildFromPlan(u: Union): Seq[Vertex] = {
    val firstSubGraph = buildFromPlan(u.children.head)

    // Create the scope
    val scope = firstSubGraph.map {
      case cc: NFColumn =>
        val newCol = catalog.getColumn(cc.name)
        newCol
    }
    // Manage lynks
    u.children.map(buildFromPlan).foreach { case sScope =>
      (sScope zip scope).foreach { case (c1: NFColumn, c2: NFColumn) => c1.usedFor += c2 } // assume that we get same columns than in the first sub graph
      sScope
    }

    scope
  }


  private def buildFromPlan(c: CreateTable): Seq[Vertex] = {
    val tableIdentifier = c.tableDesc.identifier
    val table = catalog.getTable(tableIdentifier)
    c.query match {
      case Some(u: UnaryNode) =>
        val subGraph = buildFromPlan(u)
        subGraph.map {
          case cc: NFColumn =>
            val newCol = catalog.getColumn(cc.name, table)
            cc.usedFor += newCol // Lynk
            newCol
        }

      //case Some(u: Project) => buildFromPlan(u)
      //case Some(u: Distinct) => buildFromPlan(u)
      //case Some(u: Aggregate) => buildFromPlan(u)
      case None => {
        //Quand la création est basée sur une description de champs
        c.tableDesc.schema.map {
          f: StructField => catalog.getColumn(f.name, table)
        }
      }
      case _ => throw new Exception("Unimplemented:\n" + c.query)
    }

  }


  ////////////////////// EXPRESSIONS ////////////////////////

  private def buildFromExpressions
  (
    inScope: Seq[Vertex],
    expressions: Seq[Expression]
  ): Seq[Vertex] = expressions.flatMap(exp => buildFromExpression(inScope, exp))

  private def buildFromExpression
  (
    inScope: Seq[Vertex],
    exp: Expression
  ): Seq[Vertex] = exp match {

    /* LEAF Expression */
    case l: Literal => Seq.empty

    case f: UnresolvedAttribute => Seq(catalog.getColumn(f.nameParts, inScope))

    case s: UnresolvedStar => // TODO : Après constitution du catalogue, gérer les * pour lier l'ensemble des colones source / cible
      Seq(catalog.getColumn(s.target.getOrElse(Seq.empty) :+ "*", inScope))

    /* Expressions */
    case f: UnresolvedFunction => {
      val col = catalog.getColumn(f.name.funcName + "()") // TODO: ici distinguer colonne de fonction
      val scope: Seq[Vertex] = buildFromExpressions(inScope, f.children)
      scope.foreach {
        case c: NFColumn => c.usedFor += col
        case _ => throw new Exception("Unimplemented:\n" + exp)
      }
      Seq(col)
    }

    case c: CaseWhen =>
      // parcourir l'ensemble des (cases , When) et consolider
      val branches = c.branches.flatMap {
        case (cas, whe) => Seq(
          buildFromExpression(inScope, cas),
          buildFromExpression(inScope, whe)
        )
      }.flatten
      //val elseBranch = buildFromExpressions(sources, c.elseValue)
      // TODO: deal with Options(Expression)
      branches

    /* UnaryExpression */
    case u: UnaryExpression => buildFromUnaryExpression(inScope, u)

    /* Binary Expression */
    /* EqualTo  Divide  Multiply  Add  Subtract And GreaterThan GreaterThanOrEqual ...*/
    case b: BinaryExpression => buildFromBinaryExpression(inScope, b)

    /* Predicate */
    case i: In => buildFromExpression(inScope, i.value)

    case w: WindowExpression => buildFromExpression(inScope, w.windowFunction)

    case _ => throw new Exception("Unimplemented:\n" + exp)
  }

  def buildFromBinaryExpression[T <: BinaryExpression]
  (
    inScope: Seq[Vertex],
    b: T
  ): Seq[Vertex] =
    buildFromExpressions(inScope, Seq(b.left, b.right))

  def buildFromUnaryExpression
  (
    inScope: Seq[Vertex],
    u: UnaryExpression
  ): Seq[Vertex] = u match {
    case n: NamedExpression => // Alias & UnresolvedAlias
      val in = buildFromExpression(inScope, n.child)
      val out = catalog.getColumn(n match {
        case a: Alias => a.name
        case _: UnresolvedAlias => "UnresolvedAlias"
      })
      in.distinct.foreach {
        case c: NFColumn => c.usedFor += out
      } // add Links, removing duplicates (local duplicates only)
      Seq(out)

    //_: IsNull | _: IsNotNull | _: Cast | _: Not
    case _ =>
      buildFromExpression(inScope, u.child)
  }


}
