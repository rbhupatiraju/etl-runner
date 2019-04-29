package com.tklake.service

import java.util.HashMap

import com.tklake.bean.TargetTableBean

object TargetTableService {

  var tgtTableMap : HashMap[String, TargetTableBean] = new HashMap[String, TargetTableBean]

  /**
    * Add the Target Table Bean to the Target Table Map
    *
    * @param tgtTableName
    * @param tgtTableBean
    * @return
    */
  def addTargetTable(tgtTableName: String, tgtTableBean: TargetTableBean)
  = tgtTableMap.put(tgtTableName, tgtTableBean)

  /**
    * Get the Target Table Bean
    *
    * @param tgtTableName
    */
  def getTargetTableDef(tgtTableName: String) : TargetTableBean = tgtTableMap.get(tgtTableName)

}
