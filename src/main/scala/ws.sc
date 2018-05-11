
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
val json = List(1, 2, 3)
compact(render(json))


