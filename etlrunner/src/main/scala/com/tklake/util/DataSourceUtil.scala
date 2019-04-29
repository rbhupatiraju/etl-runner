package com.tklake.util

import java.util.HashMap

import com.tklake.bean.DataSourceBean

object DataSourceUtil {

  var dataSourceMap : HashMap[String, DataSourceBean] = new HashMap[String, DataSourceBean]

  /**
    * Add DataSource to the Map
    *
    * @param dataSourceName
    * @param dataSource
    * @return
    */
  def addDataSource(dataSourceName: String, dataSource: DataSourceBean)
  = dataSourceMap.put(dataSourceName, dataSource)

  /**
    * Get the DataSource from the Map
    *
    * @param dataSourceName
    * @return
    */
  def getDataSource(dataSourceName: String)
  : DataSourceBean = dataSourceMap.get(dataSourceName)

  /**
    * Print the datasource details
    */
  def printDataSource = {
    println(dataSourceMap)
  }
}
