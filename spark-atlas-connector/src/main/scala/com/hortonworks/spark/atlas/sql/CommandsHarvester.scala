/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas.sql

import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

import scala.util.Try
import org.apache.spark.sql.catalyst.{TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.hive.execution._
import org.apache.spark.sql.kafka010.atlas.ExtractFromDataSource
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.execution.datasources.v2.{MicroBatchScanExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite
import com.hortonworks.spark.atlas.{AtlasClientConf, SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.hortonworks.spark.atlas.sql.SparkExecutionPlanProcessor.SinkDataSourceWriter
import com.hortonworks.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.hortonworks.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.streaming.{SinkProgress}

object CommandsHarvester extends AtlasEntityUtils with Logging {
  override val conf: AtlasClientConf = new AtlasClientConf

  object InsertIntoHiveTableHarvester extends Harvester[InsertIntoHiveTable] {
    override def harvest(
        node: InsertIntoHiveTable,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // source tables entities
      logger.info("####Using InsertIntoHiveTableHarvester")
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      // new table entity

      val outputEntities = {
        if (node.partition.isEmpty)
          Seq(tableToEntity(node.table))
        else {
          val partKey = node.partition.head._1 //FIXME: Generalize for  multi-level  partitioning
          val partVal = node.partition.head._2 match {
            case Some(p) => p
            case _ => "N/A"

          }
          Seq(partitionToEntity(node.table,None,partKey,partVal))
        }
      }
      logger.info(s"Adding output entities: ${outputEntities(0).qualifiedName}")

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object InsertIntoHadoopFsRelationHarvester extends Harvester[InsertIntoHadoopFsRelationCommand] {
    override def harvest(
        node: InsertIntoHadoopFsRelationCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      logger.info("####Using InsertIntoHadoopFsRelationHarvester")
      // source tables/files entities
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      // new table/file entity
//      val outputEntities = Seq(node.catalogTable.map(tableToEntity(_)).getOrElse(
//        external.pathToEntity(node.outputPath.toUri.toString)))

      val outputEntities = {
        if (node.staticPartitions.isEmpty)
          Seq(node.catalogTable.map(tableToEntity(_)).getOrElse(
              external.pathToEntity(node.outputPath.toUri.toString)))
        else {
          val partKey = node.staticPartitions.head._1 //FIXME: Generalize for  multi-level  partitioning
          val partVal = node.staticPartitions.head._2
          logger.info(s"Adding lineage information for ${partKey}=${partVal}")
          Seq(partitionToEntity(node.catalogTable.head,None,partKey,partVal))
        }
      }
      logger.info(s"Adding output entities: ${outputEntities(0).qualifiedName}")


      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object CreateHiveTableAsSelectHarvester extends Harvester[CreateHiveTableAsSelectCommand] {
    override def harvest(
        node: CreateHiveTableAsSelectCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      logger.info("####Using CreateHiveTableAsSelectHarvester")
      // source tables entities
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      // new table entity
      val outputEntities = Seq(tableToEntity(node.tableDesc.copy(owner = SparkUtils.currUser())))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object CreateTableHarvester extends Harvester[CreateTableCommand] {
    override def harvest(
        node: CreateTableCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      Seq(tableToEntity(node.table))
    }
  }

  object CreateDataSourceTableAsSelectHarvester
    extends Harvester[CreateDataSourceTableAsSelectCommand] {
    override def harvest(
        node: CreateDataSourceTableAsSelectCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      logger.info("####Using CreateDataSourceTableAsSelectHarvester")

      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)
      val outputEntities = Seq(tableToEntity(node.table))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object LoadDataHarvester extends Harvester[LoadDataCommand] {
    override def harvest(
        node: LoadDataCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      logger.info("####Using LoadDataHarvester")

      val inputEntities = Seq(external.pathToEntity(node.path))
      val outputEntities = Seq(prepareEntity(node.table))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object InsertIntoHiveDirHarvester extends Harvester[InsertIntoHiveDirCommand] {
    override def harvest(
        node: InsertIntoHiveDirCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      logger.info("####Using InsertIntoHiveDirHarvester")

      if (node.storage.locationUri.isEmpty) {
        throw new IllegalStateException("Location URI is illegally empty")
      }

      val inputEntities = discoverInputsEntities(qd.qe.sparkPlan, qd.qe.executedPlan)
      val outputEntities = Seq(external.pathToEntity(node.storage.locationUri.get.toString))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object CreateViewHarvester extends Harvester[CreateViewCommand] {
    override def harvest(
        node: CreateViewCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // from table entities
      val child = node.child.asInstanceOf[Project].child
      val inputEntities = child match {
        case r: UnresolvedRelation => Seq(prepareEntity(TableIdentifier(r.tableName)))
        case _: OneRowRelation => Seq.empty
        case n =>
          logWarn(s"Unknown leaf node: $n")
          Seq.empty
      }

      // new view entities
      val viewIdentifier = node.name
      val outputEntities = Seq(prepareEntity(viewIdentifier))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object CreateDataSourceTableHarvester extends Harvester[CreateDataSourceTableCommand] {
    override def harvest(
        node: CreateDataSourceTableCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // only have table entities
      Seq(tableToEntity(node.table))
    }
  }

  object SaveIntoDataSourceHarvester extends Harvester[SaveIntoDataSourceCommand] {
    override def harvest(
        node: SaveIntoDataSourceCommand,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      logger.info("####Using SaveIntoDataSourceHarvester")

      // source table entity
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)
      val outputEntities = node match {
        case SHCEntities(shcEntities) => Seq(shcEntities)
        case JDBCEntities(jdbcEntities) => Seq(jdbcEntities)
        case KafkaEntities(kafkaEntities) => kafkaEntities
        case e =>
          logWarn(s"Missing output entities: $e")
          Seq.empty
      }

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  object WriteToDataSourceV2Harvester extends Harvester[WriteToDataSourceV2Exec] {
    override def harvest(
        node: WriteToDataSourceV2Exec,
        qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      logger.info("####Using WriteToDataSourceV2Harvester")

      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      val outputEntities = node.batchWrite match {
        case w: MicroBatchWrite if w.writeSupport.isInstanceOf[SinkDataSourceWriter] =>
          discoverOutputEntities(w.writeSupport.asInstanceOf[SinkDataSourceWriter].sinkProgress)
        case  w: MicroBatchWrite if w.writeSupport.isInstanceOf[StreamingWrite] =>
          discoverOutputEntities(w.writeSupport.asInstanceOf[StreamingWrite])
        case _ => Seq.empty[SACAtlasReferenceable]
      }

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  def prepareEntity(tableIdentifier: TableIdentifier): SACAtlasReferenceable = {
    val tableName = SparkUtils.getTableName(tableIdentifier)
    val dbName = SparkUtils.getDatabaseName(tableIdentifier)
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntity(tableDef)
  }

  private def getPlanInfo(qd: QueryDetail): Map[String, String] = {
    Map("executionId" -> qd.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(qd.qe),
      "details" -> qd.qe.toString(),
      "sparkPlanDescription" -> qd.qe.sparkPlan.toString())
  }

  private def makeProcessEntities(
                                   inputsEntities: Seq[SACAtlasReferenceable],
                                   outputEntities: Seq[SACAtlasReferenceable],
                                   qd: QueryDetail): Seq[SACAtlasReferenceable] = {
    val logMap = getPlanInfo(qd)

    val cleanedOutput = cleanOutput(inputsEntities, outputEntities)

    // ml related cached object
    if (internal.cachedObjects.contains("model_uid")) {
      Seq(internal.updateMLProcessToEntity(inputsEntities, cleanedOutput, logMap))
    } else {
      // create process entity
      Seq(internal.etlProcessToEntity(inputsEntities, cleanedOutput, logMap))
    }
  }

  private def discoverInputsEntities(
      plan: LogicalPlan,
      executedPlan: SparkPlan): Seq[SACAtlasReferenceable] = {
    val tChildren = plan.collectLeaves()
    tChildren.flatMap {
      case r: HiveTableRelation => Seq(tableToEntity(r.tableMeta))
      case v: View => Seq(tableToEntity(v.desc))
      case LogicalRelation(fileRelation: FileRelation, _, catalogTable, _) =>
        catalogTable.map(tbl => Seq(tableToEntity(tbl))).getOrElse(
          fileRelation.inputFiles.flatMap(file => Seq(external.pathToEntity(file))).toSeq)
      case SHCEntities(shcEntities) => Seq(shcEntities)
      case JDBCEntities(jdbcEntities) => Seq(jdbcEntities)
      case KafkaEntities(kafkaEntities) => kafkaEntities
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  private def discoverInputsEntities(
      sparkPlan: SparkPlan,
      executedPlan: SparkPlan): Seq[SACAtlasReferenceable] = {
    sparkPlan.collectLeaves().flatMap {
      case h if h.getClass.getName == "org.apache.spark.sql.hive.execution.HiveTableScanExec" =>
        Try {
          val method = h.getClass.getMethod("relation")
          method.setAccessible(true)
          val relation = method.invoke(h).asInstanceOf[HiveTableRelation]
          Seq(tableToEntity(relation.tableMeta))
        }.getOrElse(Seq.empty)

      case f: FileSourceScanExec =>
        f.tableIdentifier.map(tbl => Seq(prepareEntity(tbl))).getOrElse(
          f.relation.location.inputFiles.flatMap(file => Seq(external.pathToEntity(file))).toSeq)
      case SHCEntities(shcEntities) => Seq(shcEntities)
      case JDBCEntities(jdbcEntities) => Seq(jdbcEntities)
      case KafkaEntities(kafkaEntities) => kafkaEntities
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  private def discoverOutputEntities(sink: SinkProgress): Seq[SACAtlasReferenceable] = {
    if (sink.description.contains("FileSink")) {
      val begin = sink.description.indexOf('[')
      val end = sink.description.indexOf(']')
      val path = sink.description.substring(begin + 1, end)
      logDebug(s"record the streaming query sink output path information $path")
      Seq(external.pathToEntity(path))
    } else if (sink.description.contains("ConsoleSinkProvider")) {
      logInfo(s"do not track the console output as Atlas entity ${sink.description}")
      Seq.empty
    } else {
      Seq.empty
    }
  }

  private def discoverOutputEntities(writer: StreamingWrite): Seq[SACAtlasReferenceable] = {
    writer match {
      case KafkaEntities(kafkaEntities) => Seq(kafkaEntities)
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  object SHCEntities {
    private val SHC_RELATION_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.hbase.HBaseRelation"

    private val RELATION_PROVIDER_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.hbase.DefaultSource"

    def unapply(plan: LogicalPlan): Option[SACAtlasEntityWithDependencies] = plan match {
      case l: LogicalRelation
        if l.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
        val baseRelation = l.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("parameters")
          .invoke(baseRelation).asInstanceOf[Map[String, String]]
        getSHCEntity(options)
      case sids: SaveIntoDataSourceCommand
        if sids.dataSource.getClass.getCanonicalName.endsWith(RELATION_PROVIDER_CLASS_NAME) =>
        getSHCEntity(sids.options)
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[SACAtlasEntityWithDependencies] = plan match {
      case r: RowDataSourceScanExec
        if r.relation.getClass.getCanonicalName.endsWith(SHC_RELATION_CLASS_NAME) =>
        val baseRelation = r.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("parameters")
          .invoke(baseRelation).asInstanceOf[Map[String, String]]
        getSHCEntity(options)
      case _ => None
    }

    def getSHCEntity(options: Map[String, String]): Option[SACAtlasEntityWithDependencies] = {
      if (options.getOrElse("catalog", "") != "") {
        val catalog = options("catalog")
        val cluster = options.getOrElse(AtlasClientConf.CLUSTER_NAME.key, clusterName)
        val jObj = parse(catalog).asInstanceOf[JObject]
        val map = jObj.values
        val tableMeta = map("table").asInstanceOf[Map[String, _]]
        // `asInstanceOf` is required. Otherwise, it fails compilation.
        val nSpace = tableMeta.getOrElse("namespace", "default").asInstanceOf[String]
        val tName = tableMeta("name").asInstanceOf[String]
        Some(external.hbaseTableToEntity(cluster, tName, nSpace))
      } else {
        None
      }
    }
  }

  object KafkaEntities {
    private def convertTopicsToEntities(
        topics: Set[KafkaTopicInformation]): Option[Seq[SACAtlasEntityWithDependencies]] = {
      if (topics.nonEmpty) {
        Some(topics.map(external.kafkaToEntity(clusterName, _)).toSeq)
      } else {
        None
      }
    }

    def unapply(plan: LogicalPlan): Option[Seq[SACAtlasEntityWithDependencies]] = plan match {
      case l: LogicalRelation if ExtractFromDataSource.isKafkaRelation(l.relation) =>
        val topics = ExtractFromDataSource.extractSourceTopicsFromKafkaRelation(l.relation)
        Some(topics.map(external.kafkaToEntity(clusterName, _)).toSeq)
      case sids: SaveIntoDataSourceCommand
        if ExtractFromDataSource.isKafkaRelationProvider(sids.dataSource) =>
        getKafkaEntity(sids.options).map(Seq(_))
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[Seq[SACAtlasEntityWithDependencies]] = {
      val topics = plan match {
        case r: RowDataSourceScanExec =>
          ExtractFromDataSource.extractSourceTopicsFromKafkaRelation(r.relation)
        case r: RDDScanExec =>
          ExtractFromDataSource.extractSourceTopicsFromDataSourceV1(r).toSet
        case r: MicroBatchScanExec =>
          ExtractFromDataSource.extractSourceTopicsFromDataSourceV2(r).toSet
        case _ => Set.empty[KafkaTopicInformation]
      }

      convertTopicsToEntities(topics)
    }

    def unapply(r: StreamingWrite): Option[SACAtlasEntityWithDependencies] =
      ExtractFromDataSource.extractTopic(r) match {
          case Some(topicInformation) => Some(external.kafkaToEntity(clusterName, topicInformation))
          case _ => None
      }

    def getKafkaEntity(options: Map[String, String]): Option[SACAtlasEntityWithDependencies] = {
      options.get("topic") match {
        case Some(topic) =>
          val cluster = options.get("kafka." + AtlasClientConf.CLUSTER_NAME.key)
          Some(external.kafkaToEntity(clusterName, KafkaTopicInformation(topic, cluster)))

        case _ =>
          // output topic not specified: maybe each output row contains target topic name
          // giving up
          None
      }
    }
  }

  object JDBCEntities {
    private val JDBC_RELATION_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation"

    private val JDBC_PROVIDER_CLASS_NAME =
      "org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider"

    def unapply(plan: LogicalPlan): Option[SACAtlasEntityWithDependencies] = plan match {
      case l: LogicalRelation
        if l.relation.getClass.getCanonicalName.endsWith(JDBC_RELATION_CLASS_NAME) =>
        val baseRelation = l.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("jdbcOptions")
          .invoke(baseRelation).asInstanceOf[JDBCOptions].parameters
        Some(getJdbcEnity(options))
      case sids: SaveIntoDataSourceCommand
        if sids.dataSource.getClass.getCanonicalName.endsWith(JDBC_PROVIDER_CLASS_NAME) =>
        Some(getJdbcEnity(sids.options))
      case _ => None
    }

    def unapply(plan: SparkPlan): Option[SACAtlasEntityWithDependencies] = plan match {
      case r: RowDataSourceScanExec
        if r.relation.getClass.getCanonicalName.endsWith(JDBC_PROVIDER_CLASS_NAME) =>
        val baseRelation = r.relation.asInstanceOf[BaseRelation]
        val options = baseRelation.getClass.getMethod("jdbcOptions")
          .invoke(baseRelation).asInstanceOf[JDBCOptions].parameters
        Some(getJdbcEnity(options))
      case _ => None
    }

    private def getJdbcEnity(options: Map[String, String]): SACAtlasEntityWithDependencies = {
      val url = options.getOrElse("url", "")
      val tableName = options.getOrElse("dbtable", "")
      external.rdbmsTableToEntity(url, tableName)
    }
  }
}
