package com.tklake.bean

class SourceBean (sourceType: String,
                  pathSQL : String,
                  dataframeName : String,
                  removeDuplicates: String) {
  
  /**
   * This method will return the source type as  
   * excel or jdbc or hdfs or file 
   *
   */
  def getSourceType : String = sourceType
  
  /**
   * This method will return the path from which the source needs to be read
   * or the SQL which needs to be run 
   */
  def getPathSQL : String =  pathSQL
  
  /**
   * This method will return the name to be assigned to the dataframe
   */
  def getDataframeName : String = dataframeName
  
  /***
   * This method will return the columns on which duplicates need to be 
   * removed 
   */
  def getRemoveDuplicates : String = removeDuplicates
  
  
  /**
   * Override the toString method 
   */
  override def toString : String = {
    s"[SOURCE_TYPE: $sourceType, PATH_SQL: $pathSQL, DATAFRAME_NAME: $dataframeName]"
  }
}