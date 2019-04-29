package com.tklake.service

import com.tklake.bean.TargetTableBean
import com.tklake.service.TargetTableService.getTargetTableDef
import com.tklake.util.DataSourceUtil.getDataSource
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.DataFrame

object DataWriteService {

  /** *
    * Write the dataframe to a table
    *
    * @param currDf
    * @param tableName
    */
  def writeToTable(currDf: DataFrame, tableName: String)
  : DataFrame = {
    val tgtTableBean = getTargetTableDef(tableName)
    tgtTableBean.getTgtTableType match {
      case "hive" => writeToHive(currDf, tgtTableBean)
      case "oracle" => writeToOracle(currDf, tgtTableBean)
      case "hbase" => writeToHbase(currDf, tgtTableBean)
      case "mysql" => writeToMySQL(currDf, tgtTableBean)
    }
    return currDf
  }

  /**
    * Write to Oracle Table
    *
    * @param df
    * @param tgtTableBean
    */
  def writeToOracle(df: DataFrame, tgtTableBean: TargetTableBean)
  = {
    //TODO: Implement this
  }

  /**
    * Write to Oracle Table
    *
    * @param df
    * @param tgtTableBean
    */
  def writeToHive(df: DataFrame, tgtTableBean: TargetTableBean)
  = {
    //TODO: Implement this
  }

  /**
    * Write to Oracle Table
    *
    * @param df
    * @param tgtTableBean
    */
  def writeToHbase(df: DataFrame, tgtTableBean: TargetTableBean)
  = {
    //TODO: Implement this
  }

  /**
    * Write to Oracle Table
    *
    * @param df
    * @param tgtTableBean
    */
  def writeToMySQL(df: DataFrame, tgtTableBean: TargetTableBean)
  = {
    val dataSource = getDataSource(tgtTableBean.getTgtTableDataSource)
    //create properties object
    val prop = new java.util.Properties
    prop.setProperty("driver", dataSource.getDriver)
    prop.setProperty("user", dataSource.getUsername)
    prop.setProperty("password", dataSource.getPassword)

    // Get the JDBC url
    val url = dataSource.getUrl

    //destination database table
    val table = tgtTableBean.getTgtTable

    //write data from spark dataframe to database
    var mode = "append"
    if (StringUtils.isEmpty(tgtTableBean.getMode)) {
      mode = tgtTableBean.getMode.toLowerCase
    }
    df.write.mode(mode).jdbc(url, table, prop)
  }
}
