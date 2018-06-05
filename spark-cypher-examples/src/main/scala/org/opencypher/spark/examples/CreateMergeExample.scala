package org.opencypher.spark.examples

import java.util.concurrent.TimeUnit.SECONDS

object CreateMergeExample extends CapsDemo {
  def demo() = {
    wait(3, SECONDS)
    cypher(
      """
        |CREATE GRAPH shapes {
        |  CONSTRUCT
        |    CREATE (s1:Square:Shape {side: 10, x: 20, y: 100})
        |    CREATE (s2:Square:Shape {side: 20, x: 0, y: 50})
        |    CREATE (c1:Circle:Shape {radius: 5, x: 5, y: 5})
        |    CREATE (c2:Circle:Shape {radius: 15, x: 0, y: 5})
        |    CREATE (s1)-[:PATH {cost: 35}]->(c1)
        |    CREATE (s2)-[:PATH]->(c2)
        |    CREATE (c1)-[:PATH]->(c2)
        |  RETURN GRAPH
        |}
      """.stripMargin)
    wait(3, SECONDS)
    cypher(
      """
        |CREATE GRAPH fools {
        |FROM GRAPH shapes
        |MATCH (s:Square)
        |CONSTRUCT
        |  MERGE (s)
        |  CREATE (s)-[:BAR]->(:Foo)
        |  CREATE (:Other)
        |  CREATE (:Other)-[:B]->(:Mid)-[:T]->(:Last)
        |MATCH (foo:Foo), (o:Other)
        |CONSTRUCT
        |  MERGE (foo)
        |  MERGE (o)
        |  CREATE (foo)-[:FOOO]->(o)
        |RETURN GRAPH
        |}
      """.stripMargin)
    wait(3, SECONDS)

    cypher(
      """
        |FROM GRAPH fools
        |MATCH (n)
        |RETURN n
      """.stripMargin)
  }
}


