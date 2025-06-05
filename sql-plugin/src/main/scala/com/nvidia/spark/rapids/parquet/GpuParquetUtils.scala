/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.parquet


import java.io.{Closeable, EOFException, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets
import java.util.{Collections, List => JList, Locale}

import scala.collection.JavaConverters._
import scala.collection.mutable

import ai.rapids.cudf.{HostMemoryBuffer, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, Path}
import org.apache.parquet.{HadoopReadOptions, ParquetReadOptions}
import org.apache.parquet.bytes.BytesUtils.readIntLittleEndian
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter.MAGIC
import org.apache.parquet.hadoop.metadata._
import org.apache.parquet.io.{InputFile, SeekableInputStream}
import org.apache.parquet.schema.MessageType

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.PartitionedFile

/**
 * A parquet compatible stream that allows reading from a HostMemoryBuffer to Parquet.
 * The majority of the code here was copied from Parquet's DelegatingSeekableInputStream with
 * minor modifications to have it be make it Scala and call into the
 * HostMemoryInputStreamMixIn's state.
 */
class HMBSeekableInputStream(
    val hmb: HostMemoryBuffer,
    val hmbLength: Long) extends SeekableInputStream
    with HostMemoryInputStreamMixIn {
  private val temp = new Array[Byte](8192)

  override def seek(offset: Long): Unit = {
    pos = offset
  }

  @throws[IOException]
  override def readFully(buffer: Array[Byte]): Unit = {
    val amountRead = read(buffer)
    val remaining = buffer.length - amountRead
    if (remaining > 0) {
      throw new EOFException("Reached the end of stream with " + remaining + " bytes left to read")
    }
  }

  @throws[IOException]
  override def readFully(buffer: Array[Byte], offset: Int, length: Int): Unit = {
    val amountRead = read(buffer, offset, length)
    val remaining = length - amountRead
    if (remaining > 0) {
      throw new EOFException("Reached the end of stream with " + remaining + " bytes left to read")
    }
  }

  @throws[IOException]
  override def read(buf: ByteBuffer): Int =
    if (buf.hasArray) {
      readHeapBuffer(buf)
    } else {
      readDirectBuffer(buf)
    }

  @throws[IOException]
  override def readFully(buf: ByteBuffer): Unit = {
    if (buf.hasArray) {
      readFullyHeapBuffer(buf)
    } else {
      readFullyDirectBuffer(buf)
    }
  }

  private def readHeapBuffer(buf: ByteBuffer) = {
    val bytesRead = read(buf.array, buf.arrayOffset + buf.position(), buf.remaining)
    if (bytesRead < 0) {
      bytesRead
    } else {
      buf.position(buf.position() + bytesRead)
      bytesRead
    }
  }

  private def readFullyHeapBuffer(buf: ByteBuffer): Unit = {
    readFully(buf.array, buf.arrayOffset + buf.position(), buf.remaining)
    buf.position(buf.limit)
  }

  private def readDirectBuffer(buf: ByteBuffer): Int = {
    var nextReadLength = Math.min(buf.remaining, temp.length)
    var totalBytesRead = 0
    var bytesRead = 0
    totalBytesRead = 0
    bytesRead = read(temp, 0, nextReadLength)
    while (bytesRead == temp.length) {
      buf.put(temp)
      totalBytesRead += bytesRead

      nextReadLength = Math.min(buf.remaining, temp.length)
      bytesRead = read(temp, 0, nextReadLength)
    }
    if (bytesRead < 0) {
      if (totalBytesRead == 0) {
        -1
      } else {
        totalBytesRead
      }
    } else {
      buf.put(temp, 0, bytesRead)
      totalBytesRead += bytesRead
      totalBytesRead
    }
  }

  private def readFullyDirectBuffer(buf: ByteBuffer): Unit = {
    var nextReadLength = Math.min(buf.remaining, temp.length)
    var bytesRead = 0
    bytesRead = 0
    bytesRead = read(temp, 0, nextReadLength)
    while (nextReadLength > 0 && bytesRead >= 0) {
      buf.put(temp, 0, bytesRead)

      nextReadLength = Math.min(buf.remaining, temp.length)
      bytesRead = read(temp, 0, nextReadLength)
    }
    if (bytesRead < 0 && buf.remaining > 0) {
      throw new EOFException("Reached the end of stream with " +
        buf.remaining + " bytes left to read")
    }
  }

  override def readFully(l: Long, bytes: Array[Byte], i: Int, i1: Int): Unit = {
    throw new RuntimeException("有大病整这么多接口")
  }

  override def readFully(l: Long, byteBuffer: ByteBuffer): Unit = {
    throw new RuntimeException("有大病整这么多接口")
  }

  override def randReadFully(list: java.util.List[java.lang.Long],
                             list1: java.util.List[ByteBuffer]): Unit = {
    throw new RuntimeException("有大病整这么多接口")
  }
}

class HMBInputFile(buffer: HostMemoryBuffer) extends InputFile with Closeable {
  override def getLength: Long = buffer.getLength

  override def newStream(): SeekableInputStream = {
    new HMBSeekableInputStream(buffer, getLength)
  }

  override def close(): Unit = {
    buffer.close()
  }
}

private[rapids] trait CopyItem {
  val length: Long
}

private[rapids] case class LocalCopy(
    channel: SeekableByteChannel,
    length: Long,
    outputOffset: Long) extends CopyItem with Closeable {
  override def close(): Unit = {
    channel.close()
  }
}

private[rapids] case class CopyRange(
    offset: Long,
    length: Long,
    outputOffset: Long) extends CopyItem

trait ParquetReadHelper extends AutoCloseable {
  val fileLength: Long
  val partFile: PartitionedFile

  def filePath(): Path

  def inputStream: SeekableInputStream

  def getFooter(conf: Configuration): ParquetMetadata

  def filterRowGroups(conf: Configuration): JList[BlockMetaData]

  def footerOffset(): Long

  def clipAndBuild(
      blocks: Seq[BlockMetaData],
      schema: MessageType,
      toDataBlockBase: Seq[BlockMetaData] => Seq[DataBlockBase]): SingleHMBAndMeta
}

object ParquetReadHelper {
  def createMemoryReadHelper(
      file: PartitionedFile,
      hadoopConf: Configuration,
      status: FileStatus,
      bounceBuffer: Array[Byte],
      execMetrics: Map[String, GpuMetric]): MemoryParquetReadHelper = {
    val fileLen = status.getLen
    val filePath = status.getPath
    val fileBuffer = withResource(new NvtxRange("cacheFile", NvtxColor.ORANGE)) { _ =>
      closeOnExcept(HostMemoryBuffer.allocate(fileLen)) { outBuf =>
        val out = new HostMemoryOutputStream(outBuf)
        withResource(filePath.getFileSystem(hadoopConf).open(filePath)) { in =>
          val range = CopyRange(0, fileLen, 0)
          GpuParquetUtils.copyDataRange(range, in, out, bounceBuffer, execMetrics)
        }
        outBuf
      }
    }
    new MemoryParquetReadHelper(file, fileLen, fileBuffer)
  }

  def fromPath(path: Path): Option[ParquetReadHelper] = {
    path match {
      case path: MemoryVirtualPath => Some(path.readHelper)
      case _ => None
    }
  }
}

class MemoryVirtualPath(val readHelper: MemoryParquetReadHelper)
  extends Path(MemoryVirtualPath.VIRTUAL_PATH_PREFIX, readHelper.partFile.filePath.toString)

private object MemoryVirtualPath {
  private val VIRTUAL_PATH_PREFIX = "in-memory@@"
}

class MemoryParquetReadHelper(
    override val partFile: PartitionedFile,
    override val fileLength: Long,
    val fileBuffer: HostMemoryBuffer) extends ParquetReadHelper with Logging {
  @transient
  private lazy val metaFilter: ParquetMetadataConverter.MetadataFilter = {
    ParquetMetadataConverter.range(partFile.start, partFile.start + partFile.length)
  }

  private var built = false

  override def filePath(): Path = new MemoryVirtualPath(this)

  override def inputStream: SeekableInputStream = new HMBInputFile(fileBuffer).newStream()

  override def getFooter(conf: Configuration): ParquetMetadata = {
    val reader = ParquetFileReader.open(new HMBInputFile(fileBuffer), buildOptions(conf))
    withResource(reader) {
      _.getFooter
    }
  }

  override def filterRowGroups(conf: Configuration): JList[BlockMetaData] = {
    val reader = ParquetFileReader.open(new HMBInputFile(fileBuffer), buildOptions(conf))
    withResource(reader) {
      _.getRowGroups
    }
  }

  override def footerOffset(): Long = {
    withResource(inputStream) { stream =>
      val footerLengthIndex = fileLength - 4 - MAGIC.length
      stream.seek(footerLengthIndex)
      val footerLength = readIntLittleEndian(stream)
      footerLengthIndex - footerLength
    }
  }

  override def clipAndBuild(
      blocks: Seq[BlockMetaData],
      schema: MessageType,
      toDataBlockBase: Seq[BlockMetaData] => Seq[DataBlockBase]): SingleHMBAndMeta = {
    require(!built, "Already built")
    built = true
    // TODO: handle the clip correctly under various conditions, especially when original footer
    // is smaller than the one to be written
    val footerPos = footerOffset()
    val clippedBuffer = fileBuffer.slice(0, fileBuffer.getLength)
    val scb = SpillableHostBuffer(clippedBuffer,
      clippedBuffer.getLength, SpillPriorities.ACTIVE_BATCHING_PRIORITY)
    val numRows = blocks.map(_.getRowCount).sum.toInt
    SingleHMBAndMeta(Array(scb),
      scb.length, numRows, toDataBlockBase(blocks), Some(footerPos))
  }

  override def close(): Unit = {
    fileBuffer.close()
  }

  private def buildOptions(conf: Configuration): ParquetReadOptions = {
    HadoopReadOptions.builder(conf).withMetadataFilter(metaFilter).build()
  }
}

object GpuParquetUtils extends Logging {
  private[rapids] val PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII)
  private[rapids] val PARQUET_CREATOR = "RAPIDS Spark Plugin"
  private[rapids] val PARQUET_VERSION = 1

  /**
   * Trim block metadata to contain only the column chunks that occur in the specified schema.
   * The column chunks that are returned are preserved verbatim
   * (i.e.: file offsets remain unchanged).
   *
   * @param readSchema the schema to preserve
   * @param blocks the block metadata from the original Parquet file
   * @param isCaseSensitive indicate if it is case sensitive
   * @return the updated block metadata with undesired column chunks removed
   */
  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  def clipBlocksToSchema(
      readSchema: MessageType,
      blocks: java.util.List[BlockMetaData],
      isCaseSensitive: Boolean): Seq[BlockMetaData] = {
    val columnPaths = readSchema.getPaths.asScala.map(x => ColumnPath.get(x: _*))
    val pathSet = if (isCaseSensitive) {
      columnPaths.map(cp => cp.toDotString).toSet
    } else {
      columnPaths.map(cp => cp.toDotString.toLowerCase(Locale.ROOT)).toSet
    }
    blocks.asScala.toSeq.map { oldBlock =>
      //noinspection ScalaDeprecation
      val newColumns = if (isCaseSensitive) {
        oldBlock.getColumns.asScala.filter(c => pathSet.contains(c.getPath.toDotString))
      } else {
        oldBlock.getColumns.asScala.filter(c =>
          pathSet.contains(c.getPath.toDotString.toLowerCase(Locale.ROOT)))
      }
      newBlockMeta(oldBlock.getRowCount, newColumns.toSeq)
    }
  }

  /**
   * Build a new BlockMetaData
   *
   * @param rowCount the number of rows in this block
   * @param columns the new column chunks to reference in the new BlockMetaData
   * @return the new BlockMetaData
   */
  def newBlockMeta(
      rowCount: Long,
      columns: Seq[ColumnChunkMetaData]): BlockMetaData = {
    val block = new BlockMetaData
    block.setRowCount(rowCount)

    var totalSize: Long = 0
    columns.foreach { column =>
      block.addColumn(column)
      totalSize += column.getTotalUncompressedSize
    }
    block.setTotalByteSize(totalSize)

    block
  }

  def copyDataRange(
      range: CopyRange,
      in: FSDataInputStream,
      out: HostMemoryOutputStream,
      copyBuffer: Array[Byte],
      execMetrics: Map[String, GpuMetric]): Long = {
    var readTime = 0L
    var writeTime = 0L
    if (in.getPos != range.offset) {
      in.seek(range.offset)
    }
    out.seek(range.outputOffset)
    var bytesLeft = range.length
    while (bytesLeft > 0) {
      // downcast is safe because copyBuffer.length is an int
      val readLength = Math.min(bytesLeft, copyBuffer.length).toInt
      val start = System.nanoTime()
      in.readFully(copyBuffer, 0, readLength)
      val mid = System.nanoTime()
      out.write(copyBuffer, 0, readLength)
      val end = System.nanoTime()
      readTime += (mid - start)
      writeTime += (end - mid)
      bytesLeft -= readLength
    }
    execMetrics.get(GpuMetric.READ_FS_TIME).foreach(_.add(readTime))
    execMetrics.get(GpuMetric.WRITE_BUFFER_TIME).foreach(_.add(writeTime))
    range.length
  }

  @scala.annotation.nowarn(
    "msg=method getPath in class ColumnChunkMetaData is deprecated"
  )
  def alignBlockMetaData(
      blocks: Seq[BlockMetaData],
      offset: Long): Seq[BlockMetaData] = {
    if (offset == 0) {
      return blocks
    }
    val outputBlocks = new mutable.ArrayBuffer[BlockMetaData](blocks.length)
    blocks.foreach { block =>
      val columns = block.getColumns.asScala
      val outputColumns = new mutable.ArrayBuffer[ColumnChunkMetaData](columns.length)
      columns.foreach { column =>
        val newDictOffset = if (column.getDictionaryPageOffset > 0) {
          column.getDictionaryPageOffset + offset
        } else {
          0
        }
        val columnSize = column.getTotalSize
        //noinspection ScalaDeprecation
        outputColumns += ColumnChunkMetaData.get(
          column.getPath,
          column.getPrimitiveType,
          column.getCodec,
          column.getEncodingStats,
          column.getEncodings,
          column.getStatistics,
          column.getStartingPos + offset,
          newDictOffset,
          column.getValueCount,
          columnSize,
          column.getTotalUncompressedSize)
      }
      outputBlocks += GpuParquetUtils.newBlockMeta(block.getRowCount, outputColumns.toSeq)
    }
    outputBlocks.toSeq
  }

  def writeFooter(
      out: OutputStream,
      blocks: Seq[BlockMetaData],
      schema: MessageType): Unit = {
    val fileMeta = new FileMetaData(schema, Collections.emptyMap[String, String],
      PARQUET_CREATOR)
    val metadataConverter = new ParquetMetadataConverter
    val footer = new ParquetMetadata(fileMeta, blocks.asJava)
    val meta = metadataConverter.toParquetMetadata(PARQUET_VERSION, footer)
    org.apache.parquet.format.Util.writeFileMetaData(meta, out)
  }
}
