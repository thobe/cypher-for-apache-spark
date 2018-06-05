package org.opencypher.spark.examples

import java.util.concurrent.TimeUnit

import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.spark.api.CAPSSession

trait CapsDemo extends App {
  private val session: CAPSSession = CAPSSession.local()

  def cypher(query: String, parameters: CypherMap = CypherMap.empty): Unit = {
    Terminal.write(query.trim, "... ")
    val result = session.cypher(experimentalMGSyntaxCreateMerge(query), parameters)
    result.show
    println()
    println()
  }

  def wait(time: Long, unit: TimeUnit): Unit = {
    Terminal.blink(">>> ", time, unit)
  }

  def demo(): Unit

  try {
    println("\u001b[2J")
    println(
      """*********************************************************
        |* Cypher for Apache Spark (CAPS) demonstration terminal *
        |*********************************************************
        |""".stripMargin)
    demo()
  } finally {
    session.sparkSession.close()
  }

  private def experimentalMGSyntaxCreateMerge(query: String): String = {
    val mergeR = """^(\s*)MERGE \((.*)\)$""".r

    val rewritten = query.lines.map {
      case mergeR(ws, variable) =>
        s"${ws}CLONE $variable"
      case other if other.trim.startsWith("CREATE") && !other.trim.startsWith("CREATE GRAPH") =>
        other.replaceFirst("CREATE", "NEW")
      case other => other
    }.mkString("\n")

    //println(rewritten)
    rewritten
  }
}
