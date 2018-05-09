package graphsql

object Parser extends SparkSqlParser(new SQLConf()) {
  def parse(command: String): LogicalPlan =
    parsePlan(VariableSubstitution.substitute(command))

}
