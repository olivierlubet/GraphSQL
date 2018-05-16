package graphsql.catalog

import graphsql.{Catalog, Column}
import graphsql.parser.QueryOutput
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.{AlterTableRenameCommand, DropTableCommand, SetCommand}
import org.apache.spark.sql.execution.datasources.CreateTable

import scala.collection.mutable.ArrayBuffer


case class CatalogBuilder(catalog: Catalog = new Catalog) {


  // TODO: passer le catalogue en param pour le rendre "fonctionnel"

  def add(plan: LogicalPlan): Catalog = {
    buildFromPlan(plan)
    catalog
  }

  private def buildFromPlan(plan: LogicalPlan): QueryOutput = plan match {
    case p: Project =>
      val sources = buildSources(p.child)
      buildFromExpressions(sources, p.projectList)

    case d: Distinct =>
      buildFromPlan(d.child)

    case a: Aggregate =>
      val sources = buildSources(a.child)
      buildFromExpressions(sources, a.aggregateExpressions)

    case c: CreateTable =>
      val table = c.tableDesc.identifier

      val subGraph = c.query match {
        // Comment traiter en une fois les "Unary Nodes" ???
        case Some(u: Project) => buildFromPlan(u)
        case Some(u: Distinct) => buildFromPlan(u)
        case Some(u: Aggregate) => buildFromPlan(u)
        case _ => throw new Exception("Unimplemented:\n" + plan)
      }
      val outScope = subGraph.scope ++ subGraph.outScope
      val scope = subGraph.scope.map { cc: Column =>
        val newCol = catalog.getColumn(
          cc.name, table.table, table.database.getOrElse("unknown")
        )
        cc.usedFor += newCol // Lynk
        newCol
      }
      QueryOutput(scope, outScope)

    // Nothing to do
    case _: DropTableCommand |
         _: AlterTableRenameCommand | // TODO : gérer correctement le AlterTable // eg: tiers -> tiers_tmp (note : ne vaut que pour AIR)
         _: SetCommand => // eg: "hive.exec.parallel=true"
      QueryOutput(Seq.empty, Seq.empty)

    case _ => throw new Exception("Unimplemented:\n" + plan)
  }

  private def buildFromExpressions
  (
    sources: Map[String, TableIdentifier],
    expressions: Seq[Expression]
  ): QueryOutput = {
    val graphs = expressions.map(exp => buildFromExpression(sources, exp))
    val scope = graphs.flatMap(g => g.scope)
    val outScope = graphs.flatMap(g => g.outScope)
    QueryOutput(scope, outScope)
  }

  private def buildFromExpression
  (
    sources: Map[String, TableIdentifier],
    exp: Expression
  ): QueryOutput = exp match {
    /* LEAF Expression */
    case l: Literal => QueryOutput(Seq.empty, Seq.empty)

    case f: UnresolvedAttribute =>
      f.nameParts.size match {
        case 1 =>
          val name = f.nameParts.head
          if (sources.size > 1) {
            println("No table specification for column " + name)
            QueryOutput(Seq(catalog.getColumn(name, "unknown", "unknown")), Seq.empty)
          } else {
            QueryOutput(Seq(catalog.getColumn(name, sources.head._2.table, sources.head._2.database.getOrElse("unknown"))), Seq.empty)
          }
        case 2 =>
          val name = f.nameParts(1)
          val table = sources(f.nameParts.head)
          QueryOutput(Seq(catalog.getColumn(name, table.table, table.database.getOrElse("unknown"))), Seq.empty)
      }


    case s: UnresolvedStar => // TODO : Après constitution du catalogue, gérer les * pour lier l'ensemble des colones source / cible
      s.target match {
        case Some(a: ArrayBuffer[String]) =>
          a.size match {
            case 1 =>
              val name = "*"
              val table = sources(a(0))
              QueryOutput(Seq(catalog.getColumn(name, table.table, table.database.getOrElse("unknown"))), Seq.empty)
          }
        case _ => throw new Exception("Unimplemented:\n" + exp)
      }

    /* Expressions */
    case f: UnresolvedFunction =>
      buildFromExpressions(sources, f.children)

    case c: CaseWhen =>
      // parcourir l'ensemble des (cases , When) et consolider
      val branches = c.branches.flatMap {
        case (cas, whe) => Seq(
          buildFromExpression(sources, cas),
          buildFromExpression(sources, whe)
        )
      }.flatMap(g => g.scope ++ g.outScope)
      //val elseBranch = buildFromExpressions(sources, c.elseValue)
      // TODO: deal with Options(Expression)
      QueryOutput(branches, Seq.empty)

    /* UnaryExpression */
    case a: Alias =>
      val in = buildFromExpression(sources, a.child)
      val out = catalog.getColumn(a.name, "select", "select")
      in.scope.foreach(c => c.usedFor += out) // add Links
      QueryOutput(Seq(out), in.scope ++ in.outScope)

    case _: IsNull | _: IsNotNull | _: Cast =>
      buildFromUnaryExpression(sources, exp.asInstanceOf[UnaryExpression])


    /* Binary Expression */
    case _: EqualTo | _: Divide | _: Multiply | _: Add =>
      buildFromBinaryExpression(sources, exp.asInstanceOf[BinaryExpression])


    case _ => throw new Exception("Unimplemented:\n" + exp)
  }

  def buildFromBinaryExpression(sources: Map[String, TableIdentifier], b: BinaryExpression): QueryOutput =
    buildFromExpressions(sources, Seq(b.left, b.right))

  def buildFromUnaryExpression(sources: Map[String, TableIdentifier], u: UnaryExpression): QueryOutput =
    buildFromExpression(sources, u.child)


  private def buildSources(plan: LogicalPlan): Map[String, TableIdentifier] = plan match {
    /* LEAF */
    case r: UnresolvedRelation =>
      Map(r.tableIdentifier.identifier -> r.tableIdentifier)

    /* BINARY NODES */
    case j: Join =>
      buildSources(j.left) ++ buildSources(j.right)

    /* UNARY NODES */
    case sa: SubqueryAlias =>
      buildSources(sa.child).map {
        case (_, ta: TableIdentifier) => sa.alias -> ta
      }

    case _: Filter |_: Project |_: Aggregate =>
      buildSources(plan.asInstanceOf[UnaryNode].child)

    case _ => throw new Exception("Unimplemented:\n" + plan)
  }

}
