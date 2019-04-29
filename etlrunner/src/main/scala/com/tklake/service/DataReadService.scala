package com.tklake.service

import java.util

import com.tklake.App.sqlContext
import com.tklake.bean.SourceBean
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.DataFrame

object DataReadService {

  /**
    * Get the source map
    *
    * @param sourceList List of SourceBean
    * @return Map of String to DataFrame
    */
  def getSourceMap(sourceList: util.ArrayList[SourceBean])
  : util.HashMap[String, DataFrame] = {
    val listIterator = sourceList.iterator()
    val sourceMap = new util.HashMap[String, DataFrame]
    while (listIterator.hasNext) {
      val sourceBean = listIterator.next()
      val sourceType = sourceBean.getSourceType
      var sourceDf: DataFrame = null
      sourceDf = sourceType match {
        case "oracle" => getDataFromOracle(sourceBean)
        case "mysql" => getDataFromMySQL(sourceBean)
        case "postgres" => getDataFromPostGres(sourceBean)
        case "excel" => getDataFromExcel(sourceBean)
        case "csv" => getDataFromCSV(sourceBean)
        case "parquet" => getDataFromParquet(sourceBean)
        case "avro" => getDataFromAvro(sourceBean)
      }

      //In case we need to drop duplicates
      if (StringUtils.isNotEmpty(sourceBean.getRemoveDuplicates)) {
        val colNames = sourceBean.getRemoveDuplicates.split(",")
        sourceDf = sourceDf.dropDuplicates(colNames)
      }

      sourceMap.put(sourceBean.getDataframeName, sourceDf)
    }
    sourceMap
  }

  /**
    * Read the data from Oracle
    *
    * @param sourceBean
    * @return Dataframe from Oracle
    */
  def getDataFromOracle(sourceBean: SourceBean)
  : DataFrame = {
    //TODO: Implement this
    println("Oracle yet to be implemented")
    null
  }

  /**
    * Read the data from MySQL
    *
    * @param sourceBean
    * @return Dataframe from MySQL
    */
  def getDataFromMySQL(sourceBean: SourceBean)
  : DataFrame = {
    //TODO: Implement this
    println("mySQL yet to be implemented")
    null
  }

  /**
    * Read the data from PostGres
    *
    * @param sourceBean
    * @return DataFrame from PostGres
    */
  def getDataFromPostGres(sourceBean: SourceBean)
  : DataFrame = {
    //TODO: Implement this
    println("PostGres yet to be implemented")
    null
  }

  /**
    * Read the data from excel
    * @param sourceBean
    * @return Dataframe from Excel
    */
  def getDataFromExcel(sourceBean: SourceBean)
  : DataFrame = {
    //TODO: Implement this
    println("Excel yet to be implemented")
    null
  }

  /**
    * Read the data from parquet file
    *
    * @param sourceBean
    * @return DataFrame from Parquet file
    */
  def getDataFromParquet(sourceBean: SourceBean)
  : DataFrame = {
    //TODO: Implement this
    println("Parquet yet to be implemented")
    null
  }

  /**
    * Read the data from avro file
    *
    * @param sourceBean
    * @return DataFrame from Avro file
    */
  def getDataFromAvro(sourceBean: SourceBean)
  : DataFrame = {
    //TODO: Implement this
    println("Avro yet to be implemented")
    null
  }

  /**
    * Read the data from CSV
    * @param sourceBean
    * @return DataFrame from CSV
    */
  def getDataFromCSV(sourceBean: SourceBean)
  : DataFrame = {
    sqlContext.read.format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load(sourceBean.getPathSQL)
  }
}
