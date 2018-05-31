package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.neo4j.CommunityNeo4jGraphDataSource
import org.opencypher.spark.examples.Neo4jHelpers._

object CreateMergeExample extends App {
  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  val neoServer = Neo4jHelpers.startNeo4j(
    """
      |CREATE (s1:Square:Shape {side: 10, x: 20, y: 100})
      |CREATE (s2:Square:Shape {side: 20, x: 0, y: 50})
      |CREATE (c1:Circle:Shape {radius: 5, x: 5, y: 5})
      |CREATE (c2:Circle:Shape {radius: 15, x: 0, y: 5})
      |CREATE (s1)-[:PATH {cost: 3.5}]->(c1)
      |CREATE (s2)-[:PATH]->(c2)
      |CREATE (c1)-[:PATH]->(c2)
    """.stripMargin)

  // Register Graph Data Sources (GDS)
  session.registerSource(Namespace("map"), CommunityNeo4jGraphDataSource(neoServer.dataSourceConfig))

  val graph = session.cypher(
    """
      |FROM GRAPH map.graph
      |MATCH (s:Square)
      |CONSTRUCT
      |  MERGE (s)
      |  CREATE (s)-[:BAR]->(:Foo)
      |  CREATE (:Other)
      |  CREATE (:Other)-[:B]->(:Mid)-[:T]->(:Last)
      |RETURN GRAPH
    """.stripMargin).getGraph

  graph.cypher(
    """
      |MATCH (n)
      |RETURN n
    """.stripMargin).show

  neoServer.close()
  session.sparkSession.close()
}
