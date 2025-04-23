/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
 {"spark": "320"}
 {"spark": "321"}
 {"spark": "321cdh"}
 {"spark": "322"}
 {"spark": "323"}
 {"spark": "324"}
 {"spark": "330"}
 {"spark": "330cdh"}
 {"spark": "330db"}
 {"spark": "331"}
 {"spark": "332"}
 {"spark": "332cdh"}
 {"spark": "333"}
 {"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.hive.rapids.shims.bd

import java.io.IOException

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.HiveStatsUtils
import org.apache.hadoop.hive.metastore.Warehouse

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{bucket, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTablePartition, ExternalCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.datasources.{BucketingUtils, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.WriteResult
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.InsertIntoHiveTableEndHook
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.materialized.column.utils.MaterializedColumnUtils
import org.apache.spark.sql.materialized.view.utils.MaterializedViewUtils
import org.apache.spark.sql.util.CatalogPartitionUtils

trait GpuInsertHiveTableHelper extends Logging {
  val newAddedPartitionsMap: mutable.Map[TablePartitionSpec, CatalogTablePartition] =
    mutable.Map.empty[TablePartitionSpec, CatalogTablePartition]

  // partitions which need to update in the end
  val partitionSetNeedToUpdate = mutable.Set.empty[TablePartitionSpec]

  def loadAndGetDynamicPartitions(
      table: CatalogTable,
      tmpLocation: Path,
      partitionSpec: Map[String, String],
      overwrite: Boolean,
      numDynamicPartitions: Int,
      writeResult: WriteResult,
      externalCatalog: ExternalCatalog,
      partition: Map[String, Option[String]],
      conf: SQLConf): Unit = {
    val updateNumRows = conf.partitionRownumberCollectEnabled &&
      conf.dynamicPartitionRownumberCollectEnabled
    val numRowMap = writeResult.partitionRows

    val pureStaticPartition = partition.filter(_._2.isDefined).map(p => (p._1, p._2.get))
    val existedPartitions = if (updateNumRows && !overwrite) {
      def partitionExists(part: String) = {
        val partSpec = pureStaticPartition ++
          Warehouse.makeSpecFromName(part).asScala.toMap
        externalCatalog.getPartitionOption(table.database, table.identifier.table, partSpec)
          .isDefined
      }

      numRowMap.keys.filter(partitionExists).toSet
    } else {
      null
    }

    val start = System.currentTimeMillis()
    val addedPartitions = externalCatalog.loadAndGetDynamicPartitions(
      db = table.database,
      table = table.identifier.table,
      tmpLocation.toString,
      partitionSpec,
      overwrite,
      numDynamicPartitions)

    val end = System.currentTimeMillis()
    logInfo(s"loadDynamicPartitions takes ${(end - start)} mills with " +
      s"$numDynamicPartitions partitions")

    val updatedPartitions = mutable.Map.empty[TablePartitionSpec, CatalogTablePartition]

    // TODO: update range partition parameters

    // update partition row number if needed
    if (updateNumRows) {
      val addedPartitionsWithUpdate = addedPartitions ++ updatedPartitions
      numRowMap.toSeq.foreach {
        case (k, v) =>
          val partSpec = pureStaticPartition.toMap ++
            Warehouse.makeSpecFromName(k).asScala.toMap
          addedPartitionsWithUpdate.get(partSpec) match {
            case None =>
              logWarning("Fail to get the partition " + partitionSpec.mkString)
              Nil
            case Some(p) =>
              val oldNumRows = if (overwrite || !existedPartitions.contains(k)) {
                0L
              } else {
                p.parameters.getOrElse("numRows", "-1").toLong
              }
              val newNumRows = if (oldNumRows >= 0) {
                v + oldNumRows
              } else {
                // oldNumRows < 0 means information not available.
                -1L
              }
              updatedPartitions.put(partSpec,
                p.copy(parameters = p.parameters ++ Map("numRows" -> newNumRows.toString)))
          }
      }
    }
    val updatedKeys = updatedPartitions.keySet
    // check addedPart's format equals table
    addedPartitions
      .filter { case (_, partition) =>
        storageFormatNotEquals(partition.storage, table.storage)
      }.foreach { case (partSpec, part) =>
        if (!updatedKeys.contains(partSpec)) {
          updatedPartitions.put(partSpec, part)
        }
      }
    // partitionSetNeedToUpdate should add all updatedPartitions
    partitionSetNeedToUpdate ++= updatedPartitions.keySet
    // return all added partition with latest status(rp or numRows params)
    newAddedPartitionsMap ++= mutable.Map.empty[TablePartitionSpec, CatalogTablePartition] ++
      addedPartitions ++ updatedPartitions
  }

  def loadAndGetStaticPartitions(
      table: CatalogTable,
      tmpLocation: Path,
      partitionSpec: Map[String, String],
      overwrite: Boolean,
      inheritTableSpecs: Boolean,
      oldPart: Option[CatalogTablePartition],
      externalCatalog: ExternalCatalog,
      metrics: Map[String, SQLMetric],
      conf: SQLConf): Unit = {
    val addedPartition = externalCatalog.loadAndGetPartition(
      table.database,
      table.identifier.table,
      tmpLocation.toString,
      partitionSpec,
      isOverwrite = overwrite,
      inheritTableSpecs = inheritTableSpecs,
      isSrcLocal = false)

    if (conf.partitionRownumberCollectEnabled) {
      val oldNumRows = if (overwrite || oldPart.isEmpty) {
        0L
      } else {
        addedPartition.parameters.getOrElse("numRows", "-1").toLong
      }
      val newNumRows = if (oldNumRows >= 0) {
        metrics("numOutputRows").value + oldNumRows
      } else {
        // oldNumRows < 0 means information not available.
        -1L
      }
      val statsMap = Map("numRows" -> newNumRows.toString)
      val newPartition =
        addedPartition.copy(parameters = addedPartition.parameters ++ statsMap)
      partitionSetNeedToUpdate.add(newPartition.spec)
      newAddedPartitionsMap ++= mutable.Map(newPartition.spec -> newPartition)
    } else {
      if (storageFormatNotEquals(addedPartition.storage, table.storage)) {
        partitionSetNeedToUpdate.add(addedPartition.spec)
      }
      newAddedPartitionsMap ++= mutable.Map(addedPartition.spec -> addedPartition)
    }
  }

  def updatePartitionStats(
      sparkSession: SparkSession,
      table: CatalogTable,
      externalCatalog: ExternalCatalog): Unit = {
    if (newAddedPartitionsMap.nonEmpty) {
      // Update partition params with last_update_time
      partitionSetNeedToUpdate ++= MaterializedViewUtils
        .updateCatalogPartitionsWithLastUpdateTime(sparkSession, table, newAddedPartitionsMap)
      // Update partition params with bucket_spec
      if (table.bucketSpec.isDefined) {
        partitionSetNeedToUpdate ++= CatalogPartitionUtils
          .updateBucketSpecInCatalogTablePartitions(table, newAddedPartitionsMap)
      }
      // Update partition params with MC schema
      partitionSetNeedToUpdate ++= MaterializedColumnUtils
        .updateCatalogTablePartitionsWithMCSchema(sparkSession, table, newAddedPartitionsMap)

      // get partitions need to updated in the end
      val partitionsToUpdate = newAddedPartitionsMap
        .filter(kv => partitionSetNeedToUpdate.contains(kv._1)).values.toSeq
        .map { part =>
          if (storageFormatNotEquals(part.storage, table.storage)) {
            logWarning(s"part $part's format not equals table:[${table.storage}],repair it")
            part.copy(storage = part.storage.copy(inputFormat = table.storage.inputFormat,
              outputFormat = table.storage.outputFormat,
              serde = table.storage.serde))
          } else part
        }
      // alter partition which need to be update
      val before = System.currentTimeMillis()
      // The partition statistics should not be updated here, as after insert,
      // spark will update num rows using partitionRownumberCollectEnabled instead of
      // using existing partition statistics.
      externalCatalog.alterPartitions(table.database, table.identifier.table,
        partitionsToUpdate, updateStatistics = false)
      val end = System.currentTimeMillis()
      logInfo(s"Alter ${partitionsToUpdate.size} partitions takes ${(end - before)} mills.")
    }
  }

  def postEvent(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      overwrite: Boolean,
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      metrics: Map[String, SQLMetric]): Unit = {
    InsertIntoHiveTableEndHook.processEndHook(
      table, partition, overwrite, sparkSession, hadoopConf, newAddedPartitionsMap, metrics)
  }

  def validateBucketNum(
      table: CatalogTable,
      numDynamicPartitions: Int,
      hadoopConf: Configuration,
      tmpLocation: Path,
      partition: Map[String, Option[String]]): Unit = {
    if (table.bucketSpec.isDefined) {
      val partitionBucketSpec = bucket.partitionBucketSpec(table)
      val outputPaths = if (numDynamicPartitions > 0) {
        getValidPartitionPaths(hadoopConf, tmpLocation, numDynamicPartitions)
          .map(p => {
            val lastPartitionColumnValue = PartitioningUtils
              .parsePathFragment(p.toString.split("/").last).values.head
            (p, partitionBucketSpec(lastPartitionColumnValue))
          })
      } else if (partition.nonEmpty) {  // static insert
        Seq((tmpLocation, partitionBucketSpec(partition(table.partitionColumnNames.last).get)))
      } else { // not a partitioned table
        Seq((tmpLocation, table.bucketSpec.get.numBuckets))
      }
      BucketingUtils.cleanAndValidateBuckets(hadoopConf, true, Some(tmpLocation), outputPaths)
    }
  }

  private def getValidPartitionPaths(
                                      conf: Configuration,
                                      outputPath: Path,
                                      numDynamicPartitions: Int): Seq[Path] = {
    val validPartitionPaths = mutable.HashSet[Path]()
    try {
      val fs = outputPath.getFileSystem(conf)
      HiveStatsUtils.getFileStatusRecurse(outputPath, numDynamicPartitions, fs)
        .filter(_.isDirectory)
        .foreach(d => validPartitionPaths.add(d.getPath))
    } catch {
      case e: IOException =>
        throw new SparkException("Unable to extract partition paths from temporary output " +
          s"location $outputPath due to : ${e.getMessage}", e)
    }
    validPartitionPaths.toSeq
  }

  private def storageFormatNotEquals(
      partitionFormat: CatalogStorageFormat,
      tableFormat: CatalogStorageFormat): Boolean = {
    partitionFormat.serde != tableFormat.serde ||
      partitionFormat.inputFormat != tableFormat.inputFormat ||
      partitionFormat.outputFormat != tableFormat.outputFormat
  }
}
