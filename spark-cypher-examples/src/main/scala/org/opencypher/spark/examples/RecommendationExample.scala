package org.opencypher.spark.examples

import org.neo4j.harness.ServerControls
import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.file.FileCsvPropertyGraphDataSource
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.examples.Neo4jHelpers._

object RecommendationExample extends App {

  // 1) Create CAPS session.
  implicit val caps: CAPSSession = CAPSSession.local()

  // 2) Start two Neo4j instances and populate them with social network data.
  implicit val neo4jServerUS: ServerControls = startNeo4j(dataFixtureUS)
  implicit val neo4jServerEU: ServerControls = startNeo4j(dataFixtureEU)

  // 3) Register Property Graph Data Sources (PGDS)

  // The graph within Neo4j is partitioned into regions using a property key. Within the data source, we map each
  // partition to a separate graph name (i.e. US and EU).
  caps.registerSource(Namespace("usSocialNetwork"), new Neo4jPropertyGraphDataSource(neo4jServerUS.dataSourceConfig))
  caps.registerSource(Namespace("euSocialNetwork"), new Neo4jPropertyGraphDataSource(neo4jServerEU.dataSourceConfig))

  // File-based CSV PGDS
  caps.registerSource(Namespace("purchases"), FileCsvPropertyGraphDataSource(rootPath = s"${getClass.getResource("/csv").getFile}"))

  // 4) Start analytical workload

  /**
    * Returns a query that creates a graph containing persons that live in the same city and
    * know each other via 1 to 2 hops. The created graph contains a CLOSE_TO relationship between
    * each such pair of persons and is stored in the session catalog using the given graph name.
    */
  def cityFriendsQuery(fromGraph: String): String =
    s"""FROM GRAPH $fromGraph
       |MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person),
       |      (a)-[:KNOWS*1..2]->(b)
       |CONSTRUCT
       |  ON $fromGraph
       |  CLONE a, b
       |  NEW (a)-[:CLOSE_TO]->(b)
       |RETURN GRAPH
      """.stripMargin

  // Find persons that are close to each other in the US social network
  val usFriends = caps.cypher(cityFriendsQuery("usSocialNetwork.graph")).getGraph
  // Find persons that are close to each other in the EU social network
  val euFriends = caps.cypher(cityFriendsQuery("euSocialNetwork.graph")).getGraph

  // Union the US and EU graphs into a single graph 'allFriends' and store it in the session.
  val allFriendsName = caps.store(GraphName("allFriends"), usFriends.unionAll(euFriends))

  // Connect the social network with the products network using equal person and customer emails.
  val connectedCustomers = caps.cypher(
    s"""FROM GRAPH $allFriendsName
       |MATCH (p:Person)
       |FROM GRAPH purchases.products
       |MATCH (c:Customer)
       |WHERE c.name = p.name
       |CONSTRUCT ON purchases.products, $allFriendsName
       |  CLONE c, p
       |  NEW (c)-[:IS]->(p)
       |RETURN GRAPH
      """.stripMargin).getGraph

  // Compute recommendations for 'target' based on their interests and what persons close to the
  // 'target' have already bought and given a helpful and positive rating.
  val recommendationTable = connectedCustomers.cypher(
    s"""MATCH (target:Person)<-[:CLOSE_TO]-(person:Person),
       |       (target)-[:HAS_INTEREST]->(i:Interest),
       |       (person)<-[:IS]-(x:Customer)-[b:BOUGHT]->(product:Product {category: i.name})
       |WHERE b.rating >= 4 AND (b.helpful * 1.0) / b.votes > 0.6
       |WITH * ORDER BY product.rank
       |RETURN DISTINCT product.title AS product, target.name AS name
       |LIMIT 3
      """.stripMargin).getRecords

  // Print the results
  recommendationTable.show

  neo4jServerUS.stop()
  neo4jServerEU.stop()


  def dataFixtureUS =
    """
       CREATE (nyc:City {name: "New York City"})
       CREATE (sfo:City {name: "San Francisco"})

       CREATE (alice:Person   {name: "Alice"}  )-[:LIVES_IN]->(nyc)
       CREATE (bob:Person     {name: "Bob"}    )-[:LIVES_IN]->(nyc)
       CREATE (eve:Person     {name: "Eve"}    )-[:LIVES_IN]->(nyc)
       CREATE (carol:Person   {name: "Carol"}  )-[:LIVES_IN]->(sfo)
       CREATE (carl:Person    {name: "Carl"}   )-[:LIVES_IN]->(sfo)
       CREATE (dave:Person    {name: "Dave"}   )-[:LIVES_IN]->(sfo)

       CREATE (eve)<-[:KNOWS]-(alice)-[:KNOWS]->(bob)-[:KNOWS]->(eve)
       CREATE (carol)-[:KNOWS]->(carl)-[:KNOWS]->(dave)

       CREATE (book_US:Interest {name: "Book"})
       CREATE (dvd_US:Interest {name: "DVD"})
       CREATE (video_US:Interest {name: "Video"})
       CREATE (music_US:Interest {name: "Music"})

       CREATE (bob)-[:HAS_INTEREST]->(book_US)
       CREATE (eve)-[:HAS_INTEREST]->(dvd_US)
       CREATE (carl)-[:HAS_INTEREST]->(video_US)
       CREATE (dave)-[:HAS_INTEREST]->(music_US)
    """

  def dataFixtureEU =
    """
       CREATE (mal:City {name: "Malmö"})
       CREATE (ber:City {name: "Berlin"})

       CREATE (mallory:Person {name: "Mallory"})-[:LIVES_IN]->(mal)
       CREATE (trudy:Person   {name: "Trudy"}  )-[:LIVES_IN]->(mal)
       CREATE (trent:Person   {name: "Trent"}  )-[:LIVES_IN]->(mal)
       CREATE (oscar:Person   {name: "Oscar"}  )-[:LIVES_IN]->(ber)
       CREATE (victor:Person  {name: "Victor"} )-[:LIVES_IN]->(ber)
       CREATE (peggy:Person   {name: "Peggy"}  )-[:LIVES_IN]->(ber)

       CREATE (mallory)-[:KNOWS]->(trudy)-[:KNOWS]->(trent)
       CREATE (peggy)-[:KNOWS]->(oscar)-[:KNOWS]->(victor)

       CREATE (book_EU:Interest {name: "Book"})
       CREATE (dvd_EU:Interest {name: "DVD"})
       CREATE (video_EU:Interest {name: "Video"})
       CREATE (music_EU:Interest {name: "Music"})

       CREATE (trudy)-[:HAS_INTEREST]->(book_EU)
       CREATE (eve)-[:HAS_INTEREST]->(dvd_EU)
       CREATE (victor)-[:HAS_INTEREST]->(video_EU)
       CREATE (peggy)-[:HAS_INTEREST]->(music_EU)
    """

}
