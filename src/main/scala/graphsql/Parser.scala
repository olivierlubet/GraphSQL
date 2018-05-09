package graphsql

import graphsql.parser.VariableSubstitution
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf

object Parser extends SparkSqlParser(new SQLConf()) {
  def parse(command: String): LogicalPlan =
    parsePlan(VariableSubstitution.substitute(command))

}
