package graphsql.controler

import java.net.URL

import graphsql.util.Control

import scala.util.{Failure, Success}

object SQLFileLoader {

  def load(url: URL): List[String] = {

    Control.readURL(url) match {
      case Failure(s) =>
        println(s"Failed reading $url : $s")
        List()
      case Success(lines) => lines
        .map(s =>
          if (s.length > 0 && s.charAt(0) == '#') ""
          else eraseComment(s)
        ) // Supprimer les commentaires
        .mkString(" ") // Rassembler toutes les lignes en une seule
        .split(";") // Séparer les requêtes une à une // Bug potentiel : des ';' en chaine de caractère
        .map(_.trim) // Supprimer les espaces avant / après
        .filter(_.length > 0) // Supprimer les lignes vides
        .toList
    }
  }

  def eraseComment(str: String): String = {
    object MyState extends Enumeration {
      type State = Value
      val NORMAL, DOUBLEQUOTED_STRING, SIMPLEQUOTED_STRING, MINUS, COMMENT = Value
    }

    def eraseComment(str: String, state: MyState.Value, todo: String): String = {

      if (todo.isEmpty) str
      else {
        val c = todo.head
        state match {
          //https://www.scala-lang.org/files/archive/spec/2.11/01-lexical-syntax.html#escape-sequences
          case MyState.NORMAL => c match {
            case '\u0027' => eraseComment(str + c, MyState.SIMPLEQUOTED_STRING, todo.tail)
            case '\u0022' => eraseComment(str + c, MyState.DOUBLEQUOTED_STRING, todo.tail)
            case '-' => eraseComment(str + c, MyState.MINUS, todo.tail)
            case _ => eraseComment(str + c, MyState.NORMAL, todo.tail)
          }
          case MyState.MINUS => c match {
            case '-' => eraseComment(str.take(str.length - 1), MyState.COMMENT, todo.tail) // on efface le dernier '-'
            case _ => eraseComment(str + c, MyState.NORMAL, todo.tail)
          }
          case MyState.COMMENT => c match {
            case _ => eraseComment(str, MyState.COMMENT, todo.tail)
          }
          case MyState.SIMPLEQUOTED_STRING => c match {
            case '\u0027' => eraseComment(str + c, MyState.NORMAL, todo.tail)
            case _ => eraseComment(str + c, MyState.SIMPLEQUOTED_STRING, todo.tail)
          }
          case MyState.DOUBLEQUOTED_STRING => c match {
            case '\u0022' => eraseComment(str + c, MyState.NORMAL, todo.tail)
            case _ => eraseComment(str + c, MyState.DOUBLEQUOTED_STRING, todo.tail)
          }
        }
      }
    }

    eraseComment("", MyState.NORMAL, str)
  }
}
