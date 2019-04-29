package com.tklake

import com.tklake.excelreader.ExcelTemplateReader
import com.tklake.service.ProcessTaskService
import com.tklake.configreader.ConfigReader.loadDatabaseProperties
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * @author Raghurama Raju, Bhupatiraju
  */
object App {

  var sqlContext: SQLContext = null
  var sparkSession: SparkSession = null

  /**
    * Main Starter method
    */
  def main(args: Array[String]): Unit = {
    //Loading the common conf properties
    loadDatabaseProperties

    val path = args(0)
    //Starting the reading of the excel file
    val reader = new ExcelTemplateReader()
    val taskList = reader.readExcel(path)

    //Starting the spark session
    sparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("ETLRunner")
      .getOrCreate()
    sqlContext = sparkSession.sqlContext

    //Starting the processing of the tasks defined in the excel
    val processTaskService = new ProcessTaskService
    processTaskService.processTaskList(taskList)

    //Stopping the spark session once complete
    sparkSession.stop
  }
}
