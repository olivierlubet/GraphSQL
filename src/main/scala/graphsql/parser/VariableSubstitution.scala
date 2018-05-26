package graphsql.parser

import scala.util.matching.Regex

/**
  * A helper class for performing variable substitution using the name of the variable itself ("${BAZ}" -> "baz")
  *
  * Based on
  * https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/internal/VariableSubstitution.scala
  * and
  * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/internal/config/ConfigReader.scala
  */
object VariableSubstitution {

  // "${prefix:variableName}"
  //private val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}".r
  val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)(?:\\^)*\\}".r
  // ajout du (?:\^)* pour gérer ${BIF_ENV^^}
  // pour tester : https://regex101.com/

  /**
    * Given a query, does variable substitution and return the result.
    */
  def substitute(input: String): String = {
    if (input != null) {
      REF_RE.replaceAllIn(input, { m =>
        val prefix = m.group(1)
        val name = m.group(2)
        val ref = if (prefix == null) name else s"$prefix-$name"
        Regex.quoteReplacement(ref)
      })
    } else {
      input
    }
  }
}
