package com.igniteplus.data.pipeline.cleansing
import com.igniteplus.data.pipeline.service.FileWriterService.writeFile
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lower, row_number, trim, unix_timestamp, when}


object CleanData {


  // Convert into correct datatype
  def changeDataType(df:DataFrame ,
                     colList: Seq[String],
                     dataType : Seq[String]):DataFrame = {
    var dfToDataType = df
    for( i <- colList.indices) {
      if(dataType(i) == "timestamp")
      {
        dfToDataType= dfToDataType.withColumn(colList(i),unix_timestamp(col(colList(i)),"MM/dd/yyyy H:mm").cast("double").cast(dataType(i)))
        dfToDataType.printSchema()

      }
      else
      {
        dfToDataType = dfToDataType.withColumn(colList(i), col(colList(i)).cast(dataType(i)))
      }

    }
    dfToDataType.printSchema()
    dfToDataType
  }

  //Trim the column
  def trimColumn(df:DataFrame):DataFrame = {

    var dfTrim = df
    for ( i <- df.columns) {
      dfTrim = dfTrim.withColumn(df(i).toString(),trim(col(df(i).toString())))
    }
    dfTrim.show(false)
    dfTrim
  }

 // checking for not null
//  def checkNullKeyColumns(df:DataFrame,
//                          columnList: Seq[String]):DataFrame = {
//    var dfCheckNullKey = df
//    for(i <- columnList) {
//      dfCheckNullKey = dfCheckNullKey.filter(col(i).isNull)
//    }

//    dfCheckNullKey

//  }
  
  // checking for null values and filter it and write into file
 def removeNull(df:DataFrame,columnName:Seq[String]): DataFrame ={
 var nullDf : DataFrame = df
 var notNullDf : DataFrame = df
 for (i <- columnName) {
   {
     nullDf = df.filter(df(i).isNull)
     notNullDf = df.filter(df(i).isNotNull)
   }
   if(nullDf.count()> 0) {
     FileWriterService.writeFile(nullDf,
          FILE_FORMAT,
          WRITE_PATH,
          SAVE_FILE_MODE)
 nullDf
}

// another method for removing null values
  def checkForNull (df: DataFrame, colNames:Seq[String]) : DataFrame = {

      val changedColName: Seq[Column] = colNames.map(x => col(x))
      val condition: Column = changedColName.map(x => x.isNull).reduce(_ || _)
      val dfChanged = df.withColumn("nullFlag", when(condition, "true").otherwise("false"))
    // filter the null rows from the data frame
    val  dfNullRows:DataFrame = dfChanged.filter(dfChanged("nullFlag")==="true")

    // if null rows present write into a file
    if (dfNullRows.count() > 0) {
      FileWriterService.writeFile(dfNullRows,
        FILE_FORMAT,
        WRITE_PATH,
        SAVE_FILE_MODE)
    }
    dfChanged
      

  }

  //Filter not null columns
//  def itemNotNull(df:DataFrame,
//                  columnList: Seq[String]):DataFrame={
//    val df2:DataFrame= df.na.drop(columnList)
//    df2.show()
//    df2
//  }

  //def nullColumns(df:DataFrame)={
    //val naItemId= df.filter(col("item_id").isNull)
   // naItemId.show()
   // naItemId
  //}

  // removing duplicates
  def removeDuplicates (df:DataFrame ,
                        orderByCol: String ,
                        partitionColumns : Seq[String]
                       ) : DataFrame  = {
    if (orderByCol == "event_timestamp") {
      val windowSpec = Window.partitionBy(partitionColumns.map(col): _*).orderBy(desc(orderByCol))
      val dfDropDuplicate: DataFrame = df.withColumn(colName = "row_number", row_number().over(windowSpec))
        .filter(conditionExpr = "row_number == 1").drop("row_number")
      println("Distinct count of session_id and visitor_id  and event_timestamp and item id: " + dfDropDuplicate.count())
      dfDropDuplicate
    }
    else {
      val dfDropDupItem = df.dropDuplicates(orderByCol)
      dfDropDupItem.show()
      dfDropDupItem

    }
  }



  //to lower case the column

    def toLowerCase(df: DataFrame,
                    columnList: Seq[String]): DataFrame = {
      var dfLowerClickStream = df
      for (i <- df.columns) {
        dfLowerClickStream = dfLowerClickStream.withColumn(df(i).toString(), lower(col(df(i).toString())))
      }
      dfLowerClickStream.show(false)
      dfLowerClickStream
    }
  }



