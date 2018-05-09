package graphsql.catalog

import graphsql.{Catalog, Column}
import graphsql.parser.QueryOutput
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, CaseWhen, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.execution.datasources.CreateTable


case class Builder(catalog: Catalog = new Catalog) {


  // TODO: passer le catalogue en param pour le rendre "fonctionnel"
  /*def buildGraphX(plan: LogicalPlan): Graph[String, String] = {
    val sg: QueryOutput = buildFromPlan(plan)
    val browser:Browser = new Browser(catalog)

    val vertex: Seq[(VertexId, String)] =
      browser.vertices.map(v => (v.id,v.fullName))

    val edges: Seq[Edge[String]] = browser.edges

    graphx.Graph(sc.parallelize(vertex), sc.parallelize(edges))
  }*/
  def add(plan: LogicalPlan): Catalog = {
    buildFromPlan(plan)
    catalog
  }

  private def buildFromPlan(plan: LogicalPlan): QueryOutput = plan match {
    case p: Project =>
      val sources = buildSources(p.child)
      buildFromExpressions(sources, p.projectList)

    case c: CreateTable =>
      val table = c.tableDesc.identifier

      c.query match {
        case Some(p: Project) =>
          val subGraph = buildFromPlan(p)
          val outScope = subGraph.scope ++ subGraph.outScope
          val scope = subGraph.scope.map { cc: Column =>
            val newCol = catalog.getColumn(
              cc.name, table.table, table.database.getOrElse("unknown")
            )
            cc.usedFor += newCol // Lynk
            newCol
          }

          QueryOutput(scope, outScope)

        case _ => throw new Exception("Unimplemented")
      }
    case _: DropTableCommand => // Nothing to do
      QueryOutput(Seq.empty, Seq.empty)
    case _ => throw new Exception("Unimplemented")
  }

  private def buildFromExpressions
  (
    sources: Map[String, TableIdentifier],
    expressions: Seq[Expression]
  ): QueryOutput = {
    //val columns: Seq[(Option[Column], Seq[Column])] =
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
    case l: Literal => QueryOutput(Seq.empty, Seq.empty)
    case e: EqualTo =>
      val l = buildFromExpression(sources, e.left)
      val r = buildFromExpression(sources, e.right)
      QueryOutput(l.scope ++ r.scope, r.outScope ++ l.outScope)

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

    case a: Alias =>
      val in = buildFromExpression(sources, a.child)
      val out = catalog.getColumn(a.name, "select", "select")
      in.scope.foreach(c => c.usedFor += out) // add Links

      QueryOutput(Seq(out), in.scope ++ in.outScope)

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
        case _ => throw new Exception("Unimplemented")
      }
    case _ => throw new Exception("Unimplemented")

  }

  private def buildSources(plan: LogicalPlan): Map[String, TableIdentifier] = plan match {
    case r: UnresolvedRelation =>
      Map(r.tableIdentifier.identifier -> r.tableIdentifier)
    case j: Join =>
      buildSources(j.left) ++ buildSources(j.right)
    case sa: SubqueryAlias =>
      buildSources(sa.child).map {
        case (_, ta: TableIdentifier) => sa.alias -> ta
      }
    case _ => throw new Exception("Unimplemented")
  }
}
