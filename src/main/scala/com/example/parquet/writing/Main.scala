package com.example.parquet.writing

import com.example.parquet.writing.Schema.Event

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.proto.{ProtoWriteSupport, ProtoParquetWriter}
import org.apache.hadoop.fs.Path

import java.util.UUID

object Main {
  private val NumberOfRecords = 100 * 1000
  private val rnd = new scala.util.Random

  private def createS3ParquetWriter(path: String, accessKey: String, secretKey: String) = {
    val writeSupport = new ProtoWriteSupport[Event](classOf[Event])

    val compressionCodecName = CompressionCodecName.GZIP

    val blockSize = 256 * 1024 * 1024
    val pageSize = 1 * 1024 * 1024

    val outputPath = new Path(path)

    val conf = new Configuration
    conf.set("fs.s3a.access.key", accessKey)
    conf.set("fs.s3a.secret.key", secretKey)

    new ParquetWriter[Event](outputPath,
      writeSupport, compressionCodecName, blockSize, pageSize, pageSize,
      ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED, ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
      ParquetWriter.DEFAULT_WRITER_VERSION,
      conf
    )
  }

  private def createParquetWriter(path: String) = {
    val compressionCodecName = CompressionCodecName.GZIP

    val blockSize = 256 * 1024 * 1024
    val pageSize = 1 * 1024 * 1024

    val outputPath = new Path(path)

    new ProtoParquetWriter[Event](outputPath, classOf[Event], compressionCodecName, blockSize, pageSize)
  }

  private def createEvent =
    Event.newBuilder()
      .setId(UUID.randomUUID().toString)
      .setSequenceNo(rnd.nextLong())
      .setType(s"type-${rnd.nextInt(10)}")
      .setValue(rnd.nextDouble())
      .build()

  private def writeEvents(parquetWriter: ParquetWriter[Event]) = {
    val events = Iterator.fill(NumberOfRecords)(createEvent)
    events.foreach(parquetWriter.write)
    parquetWriter.close()
  }

  private def runLocal(path: String) = {
    val parquetWriter = createParquetWriter(path)
    writeEvents(parquetWriter)
  }

  private def runS3(path: String, accessKey: String, secretKey: String) = {
    val parquetWriter = createS3ParquetWriter(path, accessKey, secretKey)
    writeEvents(parquetWriter)
  }

  def main(args: Array[String]) {
    val localPath = System.getenv("LOCAL_PATH")

    if (localPath != null) {
      runLocal(localPath)
    } else {
      println("LOCAL_PATH is empty")
    }

    val s3Path = System.getenv("S3_PATH")
    val accessKey = System.getenv("AWS_ACCESS_KEY")
    val secretKey = System.getenv("AWS_SECRET_KEY")

    if (s3Path != null) {
      val path = s"s3a://$s3Path"
      runS3(path, accessKey, secretKey)
    } else {
      println("S3_PATH is empty")
    }
  }
}
