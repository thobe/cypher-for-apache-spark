/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.support

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.opencypher.caps.api.classes.Cypher
import org.opencypher.caps.api.expr.{HasLabel, Property, Var}
import org.opencypher.caps.api.ir.global.TokenRegistry
import org.opencypher.caps.api.record.{FieldSlotContent, RecordHeader}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSResult, CAPSSession}
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.{CypherMap, CypherValue}
import org.opencypher.caps.impl.convert.{fromJavaType, toSparkType}
import org.opencypher.caps.impl.physical.RuntimeContext
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.{BaseTestSuite, CAPSTestSession, SparkTestSession}
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.Element
import org.scalatest.Assertion

import scala.collection.Bag
import scala.collection.immutable._
import scala.collection.JavaConverters._

trait GraphMatchingTestSupport {

  self: BaseTestSuite with SparkTestSession.Fixture with CAPSTestSession.Fixture =>

  implicit val bagConfig = Bag.configuration.compact[CypherMap]
  val DEFAULT_LABEL = "DEFAULT"
  val sparkSession = session

  implicit val context: RuntimeContext = RuntimeContext.empty

  implicit class GraphMatcher(graph: CAPSGraph) {
    def shouldMatch(expectedGraph: CAPSGraph): Assertion = {
      val expectedNodeIds = expectedGraph.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
      val expectedRelIds = expectedGraph.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet

      val actualNodeIds = graph.nodes("n").data.select("n").collect().map(_.getLong(0)).toSet
      val actualRelIds = graph.relationships("r").data.select("r").collect().map(_.getLong(0)).toSet

      expectedNodeIds should equal(actualNodeIds)
      expectedRelIds should equal(actualRelIds)
    }
  }

  case class TestGraph(gdl: String)(implicit caps: CAPSSession) {

    private val queryGraph = new GDLHandler.Builder()
      .setDefaultEdgeLabel(DEFAULT_LABEL)
      .setDefaultVertexLabel(DEFAULT_LABEL)
      .buildFromString(gdl)

    def cypher(query: String): CAPSResult =
      caps.cypher(graph, query, Map.empty)

    def cypher(query: String, parameters: Map[String, CypherValue]): CAPSResult =
      caps.cypher(graph, query, parameters)

    lazy val graph: CAPSGraph = new CAPSGraph {
      self =>

      override def graph: CAPSGraph = this
      override def session: CAPSSession = caps

      private def extractFromElement(e: Element) = e.getLabels.asScala.map { label =>
        label -> e.getProperties.asScala.map {
          case (name, prop) => name -> fromJavaType(prop)
        }
      }

      override val schema: Schema = {
        val labelAndProps = queryGraph.getVertices.asScala.flatMap(extractFromElement)
        val typesAndProps = queryGraph.getEdges.asScala.flatMap(extractFromElement)

        val schemaWithLabels = labelAndProps.foldLeft(Schema.empty) {
          case (acc, (label, props)) => acc.withNodePropertyKeys(label)(props.toSeq: _*)
        }

        typesAndProps.foldLeft(schemaWithLabels) {
          case (acc, (t, props)) => acc.withRelationshipPropertyKeys(t)(props.toSeq: _*)
        }
      }

      override val tokens = CAPSRecordsTokens(TokenRegistry.fromSchema(schema))

      override def nodes(name: String, cypherType: CTNode): CAPSRecords = {
        val header = RecordHeader.nodeFromSchema(Var(name)(cypherType), schema, tokens.registry,
          cypherType.labels.filter(_._2).keySet)

        val data = {
          val nodes = queryGraph.getVertices.asScala
            .filter(v => v.getLabels.containsAll(cypherType.labels.filter(_._2).keySet.asJava))
            .map { v =>
              val exprs = header.slots.map(_.content.key)
              val labelFields = exprs.collect {
                case HasLabel(_, label) => v.getLabels.contains(label.name)
              }
              val propertyFields = exprs.collect {
                case p@Property(_, k) =>
                  val pValue = v.getProperties.get(k.name)
                  if (fromJavaType(pValue) == p.cypherType) pValue
                  else null
              }

              Row(v.getId +: (labelFields ++ propertyFields): _*)
            }.toList.asJava

          val fields = header.slots.map { s =>
            StructField(context.columnName(s), toSparkType(s.content.cypherType))
          }

          sparkSession.createDataFrame(nodes, StructType(fields))
        }
        CAPSRecords.create(header, data)
      }

      override def relationships(name: String, cypherType: CTRelationship): CAPSRecords = {

        val header = RecordHeader.relationshipFromSchema(Var(name)(cypherType), schema, tokens.registry)

        val data = {
          val rels = queryGraph.getEdges.asScala
            .filter(e => cypherType.types.asJava.isEmpty || cypherType.types.asJava.containsAll(e.getLabels))
            .map { e =>
              val staticFields = Seq(e.getSourceVertexId, e.getId, tokens.relTypeId(e.getLabel).toLong, e.getTargetVertexId)

              val propertyFields = header.slots.slice(4, header.slots.size).map(_.content.key).map {
                case Property(_, k) => e.getProperties.get(k.name)
                case _ => throw new IllegalArgumentException("Only properties expected in the header")
              }

              Row(staticFields ++ propertyFields: _*)
          }.toList.asJava

          val fields = header.slots.map { s =>
            StructField(context.columnName(s), toSparkType(s.content.cypherType))
          }

          sparkSession.createDataFrame(rels, StructType(fields))
        }
        CAPSRecords.create(header, data)
      }
    }
  }

  // TODO: Move to RecordMatchingTestSupport
  implicit class RichRecords(records: CAPSRecords) {
    import org.opencypher.caps.impl.instances.spark.RowUtils._

    def toMaps: Bag[CypherMap] = {
      val rows = records.toDF().collect().map { r =>
        val properties = records.header.slots.map { s =>
          s.content match {
            case f: FieldSlotContent => f.field.name -> r.getCypherValue(f.key, records.header)
            case x => x.key.withoutType -> r.getCypherValue(x.key, records.header)
          }
        }.toMap
        CypherMap(properties)
      }
      Bag(rows: _*)
    }
  }
}
