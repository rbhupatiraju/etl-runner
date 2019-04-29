package com.tklake.bean

class TargetTableBean(tgtTable: String,
                      tgtTablePartitionColumn: String,
                      tgtTableType: String,
                      tgtTableDataSource: String,
                      mode: String) {

  /**
    * Get the Target Table Name
    *
    * @return
    */
  def getTgtTable : String = tgtTable

  /**
    * Get the Target Table Partition Column
    *
    * @return
    */
  def getTgtTablePartitionColumn: String = tgtTablePartitionColumn

  /**
    * Get the Target Table Type
    *
    * @return
    */
  def getTgtTableType: String = tgtTableType

  /**
    * Get the Target Table DataSource
    *
    * @return
    */
  def getTgtTableDataSource: String = tgtTableDataSource

  /**
    * Get the mode in which the table needs to be inserted
    *
    * @return
    */
  def getMode: String = mode

  /**
    * Overriden toString method
    *
    * @return
    */
  override def toString : String = {
    s"[TARGET_TABLE: ${tgtTable}, " +
      s"TARGET_TABLE_PARTITION_COLUMN: ${tgtTablePartitionColumn}, " +
      s"TARGET_TABLE_TYPE: ${tgtTableType}, " +
      s"TARGET_TABLE_DATASOURCE: ${tgtTableDataSource}" +
      s"MODE: ${mode}]"
  }

}
