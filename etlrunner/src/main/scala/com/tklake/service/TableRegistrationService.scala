package com.tklake.service

import java.util.HashMap

import com.tklake.App.sqlContext
import org.apache.spark.sql.DataFrame

object TableRegistrationService {

  /**
    * Register the source as temp tables
    */
  def registerTempTables(sourceMap: HashMap[String, DataFrame]) = {
    val entryIterator = sourceMap.entrySet().iterator()

    while (entryIterator.hasNext()) {
      val mapEntry = entryIterator.next()

      //Register the dataframe as temp table
      if (mapEntry != null && mapEntry.getValue != null) {
        mapEntry.getValue.createOrReplaceTempView(mapEntry.getKey)
      }
    }
  }

  /**
    * Unregister the source as temp tables
    */
  def unregisterTempTables(sourceMap: HashMap[String, DataFrame]) = {
    val entryIterator = sourceMap.entrySet().iterator()

    while (entryIterator.hasNext()) {
      val mapentry = entryIterator.next()

      // Unregister the temp table
      if (mapentry.getKey != null) {
        sqlContext.dropTempTable(mapentry.getKey)
      }
    }
  }
}
