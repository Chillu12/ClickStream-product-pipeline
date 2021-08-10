package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{INPUT_LOCATION_CLICK_STREAM, INPUT_WRITE_DATA}
import com.igniteplus.data.pipeline.exception.{FileReadException, FileWriteException}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriterService {
def writeFile(df:DataFrame,
              fileFormate:String,
              fileSaveMode:String,
              filePath:String): Unit = {


  val dfWriteData: Unit = {
    try {
      df.write.format(fileFormate).mode(fileSaveMode).save(filePath)
      df
    }
    catch {
      case e: Exception =>
        FileWriteException("unable to write files in the given location" + filePath)
       
    }

  }

    if (dfWriteData == 0) {
      throw FileReadException("no file read from the reader " +filePath)
    }
    
  }
}



