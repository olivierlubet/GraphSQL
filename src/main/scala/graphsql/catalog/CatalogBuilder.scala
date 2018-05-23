package graphsql.catalog

import graphsql._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.CreateTable

import scala.collection.mutable.ArrayBuffer


case class CatalogBuilder(catalog: NFCatalog = new NFCatalog) {


  // TODO: passer le catalogue en param pour le rendre "fonctionnel"

  def add(plan: LogicalPlan): NFCatalog = {
    //println(plan)
    buildFromPlan(plan)
    catalog
  }

  private def buildFromPlan(plan: LogicalPlan): Seq[Vertex] = plan match {
    case p: Project =>
      val scope = buildFromPlan(p.child)
      buildFromExpressions(scope, p.projectList)

    case d: Distinct =>
      buildFromPlan(d.child)

    case a: Aggregate =>
      val scope = buildFromPlan(a.child)
      buildFromExpressions(scope, a.aggregateExpressions)

    /* PLAN with subqueries */
    case u: Union => buildFromPlan(u)
    case c: CreateTable => buildFromPlan(c)

    /* UNARY NODES */
    case sa: SubqueryAlias => // FROM a AS b ou encore UNION
      buildFromPlan(sa.child).map {
        case t: NFTable =>
          NFTableAlias(sa.alias,t)
        case c:NFColumn =>
          val ret = sa.alias match {// en cas de UNION, sa.alias = "__auto_generated_subquery_name" -> remplacé par c.name
            case "__auto_generated_subquery_name" =>NFColumn(c.name)
            case _ => NFColumn(sa.alias)
          }
          c.usedFor+=ret
          ret
      }
    /*{
      case (_, ta: TableIdentifier) => sa.alias -> ta
    }*/

    case _: Filter | _: Aggregate | _: Distinct | _: Sort =>
      buildFromPlan(plan.asInstanceOf[UnaryNode].child)

    /* BINARY NODES */
    case j: Join => // JOIN
      buildFromPlan(j.left) ++ buildFromPlan(j.right)

    /* LEAF */
    case r: UnresolvedRelation =>
      Seq(catalog.getTable(r.tableIdentifier))

    // Nothing to do
    case _: DropTableCommand |
         _: AlterTableRenameCommand | // TODO : gérer correctement le AlterTable // eg: tiers -> tiers_tmp (note : ne vaut que pour AIR)
         _: SetCommand => // eg: "hive.exec.parallel=true"
      Seq.empty

    case _ => throw new Exception("Unimplemented:\n" + plan)
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
    val table = c.tableDesc.identifier
    val subGraph = c.query match {
      // Comment traiter en une fois les "Unary Nodes" ???
      case Some(u: Project) => buildFromPlan(u)
      case Some(u: Distinct) => buildFromPlan(u)
      case Some(u: Aggregate) => buildFromPlan(u)
      case _ => throw new Exception("Unimplemented:\n" + c.query)
    }
    val scope = subGraph.map { case cc: NFColumn =>
      val newCol = catalog.getColumn(
        cc.name, table.table, table.database
      )
      cc.usedFor += newCol // Lynk
      newCol
    }
    scope
  }


  ////////////////////// EXPRESSIONS ////////////////////////

  private def buildFromExpressions
  (
    inScope: Seq[Vertex],
    expressions: Seq[Expression]
  ): Seq[Vertex] = {
    val graphs = expressions.map(exp => buildFromExpression(inScope, exp))
    graphs.flatten
  }

  private def buildFromExpression
  (
    inScope: Seq[Vertex],
    exp: Expression
  ): Seq[Vertex] = {

    val tables: Map[String, NFTable] = inScope.flatMap {
      case t: NFTable => Some(t)
      case _ => None
    }.map { t => t.name -> t }.toMap

    exp match {

      /* LEAF Expression */
      case l: Literal => Seq.empty

      case f: UnresolvedAttribute => Seq(catalog.getColumn(f.nameParts,inScope))


      case s: UnresolvedStar => // TODO : Après constitution du catalogue, gérer les * pour lier l'ensemble des colones source / cible
        s.target match {
          case Some(a: ArrayBuffer[String]) =>
            a.size match {
              case 1 =>
                Seq(catalog.getColumn(Seq(a.head,"*"),inScope))
            }
          case _ => throw new Exception("Unimplemented:\n" + exp)
        }

      /* Expressions */
      case f: UnresolvedFunction =>
        buildFromExpressions(inScope, f.children)

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
      case a: Alias =>
        val in = buildFromExpression(inScope, a.child)
        val out = catalog.getColumn(a.name)
        in.distinct.foreach {
          case c: NFColumn => c.usedFor += out
        } // add Links, removing duplicates (local duplicates only)
        Seq(out)

      case _: IsNull | _: IsNotNull | _: Cast | _: Not =>
        buildFromUnaryExpression(inScope, exp.asInstanceOf[UnaryExpression])


      /* Binary Expression */
      case _: EqualTo | _: Divide | _: Multiply | _: Add =>
        buildFromBinaryExpression(inScope, exp.asInstanceOf[BinaryExpression])

      case _ => throw new Exception("Unimplemented:\n" + exp)
    }
  }

  def buildFromBinaryExpression
  (
    inScope: Seq[Vertex],
    b: BinaryExpression
  ): Seq[Vertex] =
    buildFromExpressions(inScope, Seq(b.left, b.right))

  def buildFromUnaryExpression
  (
    inScope: Seq[Vertex],
    u: UnaryExpression
  ): Seq[Vertex] =
    buildFromExpression(inScope, u.child)


  //TODO: supprimer toute référence au buildSource (si inutile) et utiliser uniquement le catalogue
  private def buildSources(plan: LogicalPlan): Map[String, TableIdentifier] = plan match {
    /* PLAN */
    /* Cas intéressant : il faut lier les colonnes d'un plan supérieur à un sous plan */
    case u: Union => // UNION -> il faudra créer un alias pour chaque colonne, puis toutes les lier, autrement les tables s'écrasent
      val ret = u.children.map(buildSources).reduce(_ ++ _)
      buildFromPlan(u)
      ret

    case p: Project => // SELECT
      val ret = buildSources(p.child)
      ret

    /* LEAF */
    case r: UnresolvedRelation =>
      Map(r.tableIdentifier.identifier -> r.tableIdentifier)

    /* BINARY NODES */
    case j: Join => // JOIN
      buildSources(j.left) ++ buildSources(j.right)

    /* UNARY NODES */
    case sa: SubqueryAlias => // FROM a AS b ou encore UNION
      buildSources(sa.child).map { // en cas de UNION, "__auto_generated_subquery_name"
        case (_, ta: TableIdentifier) => sa.alias -> ta
      }

    case _: Filter | _: Aggregate | _: Distinct | _: Sort =>
      buildSources(plan.asInstanceOf[UnaryNode].child)

    case _ => throw new Exception("Unimplemented:\n" + plan)
  }

}
