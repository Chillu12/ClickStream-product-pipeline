package com.igniteplus.data.pipeline.constants

object ApplicationConstants {
  val MASTER:String="local"
  val APP_NAME:String="ClickStream Pipeline"

  //Read the file
  val INPUT_LOCATION_CLICK_STREAM:String="data/input/clickstream/clickstream_log.csv"
  val INPUT_LOCATION_ITEM:String="data/input/item/item_data.csv"

  val INPUT_WRITE_DATA : String = "data/output/merged-data/writeNullKeyCol/abc"

  val CSV_FORMAT: String = "csv"

  val SAVE_FILE_MODE: String ="overwrite"

  val WRITE_PATH :String = "data/output/merged-data/writeNullKeyCol/writeNullKeyCol.csv"

  val TIMESTAMP_TYPE : String = "timestamp"
  val FLOAT_TYPE : String = "float"
  
  
  // output write files
  val CLICK_STREAM_NULL_ROWS_DATASET: String ="data/output/pipeline_failures/clickstream_null_values"
  val ITEM_NULL_ROWS_DATASET: String ="data/output/pipeline_failures/item_null_values"




  val SESSION_ID: String = "session_id"
  val VISITOR_ID:String = "visitor_id"
  val EVENT_TIMESTAMP:String = "event_timestamp"
  val REDIRECTION_SOURCE:String = "redirection_source"
  val DEVICE_TYPE:String = "device_type"
  val CLICK_STREAM_UNIQUE_COLUMN : Seq[String] =Seq(ApplicationConstants.SESSION_ID,ApplicationConstants.VISITOR_ID)

  val ITEM_ID : String = "item_id"
  val ITEM_PRICE : String = "item_price"
  val DEPARTMENT_NAME:String = "department_name"
  val ITEM_UNIQUE_COLUMN : Seq[String] = Seq(ApplicationConstants.ITEM_ID)
  
  // exit code 
  val FAILURE_EXIT_CODE : Int = 1


  val CLICK_STREAM_VALID_DATATYPE_COLUMNS:Seq[String] = Seq(ApplicationConstants.EVENT_TIMESTAMP)
  val CLICK_STREAM_VALID_DATATYPE:Seq[String] = Seq(ApplicationConstants.TIMESTAMP_TYPE)
  val CLICK_STREAM_UNIQUE_COLUMNS:Seq[String] = Seq(ApplicationConstants.SESSION_ID , ApplicationConstants.ITEM_ID)
  val CLICK_STREAM_LOWERCASE_COLUMNS:Seq[String] = Seq(ApplicationConstants.REDIRECTION_SOURCE , ApplicationConstants.DEVICE_TYPE )

  val ITEM_VALID_DATATYPE_COLUMNS:Seq[String] = Seq(ApplicationConstants.ITEM_PRICE)
  val ITEM_VALID_DATATYPE:Seq[String] = Seq(ApplicationConstants.FLOAT_TYPE)
  val ITEM_UNIQUE_COLUMNS:Seq[String] = Seq(ApplicationConstants.ITEM_ID)
  val ITEM_LOWERCASE_COLUMNS:Seq[String] = Seq(ApplicationConstants.DEPARTMENT_NAME)

}
