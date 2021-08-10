package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.INPUT_LOCATION_CLICK_STREAM
import com.igniteplus.data.pipeline.exception.FileReadException
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.igniteplus.data.pipeline.util.ApplicationUtil


object FileReaderService {
  def readFile(path: String)(implicit spark: SparkSession): DataFrame = {

    val dfClickStream: DataFrame = {
      try {
        spark.read.option("header", "true").format(source = "csv").load(path)
        
      } catch {
        case e: Exception =>
          FileReadException("unable to read files in the given location " + s"$INPUT_LOCATION_CLICK_STREAM")
          spark.emptyDataFrame
      }
    }
      val dfClickStreamCount: Long = dfClickStream.count()

      if (dfClickStreamCount == 0) {
        throw FileReadException("no file read from the reader " + s"$INPUT_LOCATION_CLICK_STREAM")
      }
      
    }

}



