package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{INPUT_LOCATION_CLICK_STREAM, INPUT_WRITE_DATA}
import com.igniteplus.data.pipeline.exception.{FileReadException, FileWriteException}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriterService {
def writeFile(df:DataFrame,
              fileFormate:String,
              fileSaveMode:String,
              filePath:String)(implicit spark:SparkSession): DataFrame = {


  val dfWriteData: DataFrame = {
    try {
      df.write.format(fileFormate).mode(fileSaveMode).save(filePath)
      df
    }
    catch {
      case e: Exception =>
        FileWriteException("unable to write files in the given location" + s"$INPUT_WRITE_DATA")
        spark.emptyDataFrame
    }

  }

    val dfClickStreamCount: Long = dfWriteData.count()

    if (dfClickStreamCount == 0) {
      throw FileReadException("no file read from the reader " + s"$INPUT_WRITE_DATA")
    }
    else {
      dfWriteData
    }
  }
}



