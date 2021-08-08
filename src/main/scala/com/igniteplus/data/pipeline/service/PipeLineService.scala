package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.cleansing.CleanData
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{APP_NAME, CLICK_STREAM_LOWERCASE_COLUMNS,   CLICK_STREAM_UNIQUE_COLUMNS, CLICK_STREAM_VALID_DATATYPE, CLICK_STREAM_VALID_DATATYPE_COLUMNS, CSV_FORMAT, EVENT_TIMESTAMP, INPUT_LOCATION_CLICK_STREAM, INPUT_LOCATION_ITEM, ITEM_ID, ITEM_LOWERCASE_COLUMNS, ITEM_PRICE, ITEM_UNIQUE_COLUMN, ITEM_VALID_DATATYPE, ITEM_VALID_DATATYPE_COLUMNS, MASTER}
import com.igniteplus.data.pipeline.util.ApplicationUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}

object PipeLineService {


  def executePipeLine(): Unit = {

    // creating sparkSession
   implicit val spark: SparkSession = ApplicationUtil.createSparkSession(APP_NAME, MASTER)


       // Reading file

       val dfClickStream = (FileReaderService.readFile(INPUT_LOCATION_CLICK_STREAM)(spark))
       val dfItem = FileReaderService.readFile(INPUT_LOCATION_ITEM)(spark)

       // converting into  correct data type
        val dfClickStreamDatatype=CleanData.changeDataType(dfClickStream, CLICK_STREAM_VALID_DATATYPE_COLUMNS, CLICK_STREAM_VALID_DATATYPE)
        val dfItemDatatype = CleanData.changeDataType(dfItem,  ITEM_VALID_DATATYPE_COLUMNS , ITEM_VALID_DATATYPE )

       //trim the column

        val dfTrimCase :DataFrame = CleanData.trimColumn(dfClickStreamDatatype)
        val dfTrimItemData:DataFrame = CleanData.trimColumn(dfItemDatatype)

       // lower case data
    val dfClickStreamCase: DataFrame = CleanData.toLowerCase(dfTrimCase, CLICK_STREAM_LOWERCASE_COLUMNS )
    val dfItemLowerCase:DataFrame = CleanData.toLowerCase(dfTrimItemData, ITEM_LOWERCASE_COLUMNS)

        // remove null values
        val dfFilterClickStream : DataFrame = CleanData.checkNullKeyColumns(dfClickStreamCase, CLICK_STREAM_UNIQUE_COLUMNS)
        val dfFilterItem : DataFrame = CleanData.checkForNull(dfItemLowerCase, ITEM_UNIQUE_COLUMN)

        // remove duplicates
        val dfDropClickStreamDup : DataFrame= CleanData.removeDuplicates(dfFilterClickStream, EVENT_TIMESTAMP , CLICK_STREAM_UNIQUE_COLUMNS)
        val dfDropItemDup:DataFrame = CleanData.removeDuplicates( dfFilterItem, ITEM_ID, Seq(null))





    }


}
