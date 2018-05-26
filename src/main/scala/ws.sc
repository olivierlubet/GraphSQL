import graphsql.parser.VariableSubstitution.REF_RE

import scala.util.matching.Regex


//val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}".r
val REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)(?:\\^)*\\}".r
// ajout du (?:\^)* pour gérer ${BIF_ENV^^}

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

substitute("abc${bb}ddd")


substitute("abc${bb^^}ddd")
