
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
val json = List(1, 2, 3)
compact(render(json))


val a = Seq (1->2,2->3,3->4)
a ++ Seq(1->3)

