package org.opencypher.spark.impl

object experimentalMGSyntaxCreateMerge extends (String => String) {

  override def apply(query: String): String = experimentalMGSyntaxCreateMerge(query)

  private def experimentalMGSyntaxCreateMerge(query: String): String = {
    val mergeR = """^(\s*)MERGE \((.*)\)$""".r

    val rewritten = query.lines.map {
      case mergeR(ws, variable) =>
        s"${ws}CLONE $variable"
      case other if other.trim.startsWith("CREATE") =>
        other.replaceFirst("CREATE", "NEW")
      case other => other
    }.mkString("\n")

    //    println(rewritten)
    rewritten
  }
}
