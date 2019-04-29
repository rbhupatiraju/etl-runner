package com.tklake.bean

import java.util.ArrayList

class TaskBean (taskName: String,
                taskSQL: String,
                dataframeName: String,
                postProcessingPattern: String) {
  
  var sourceList : ArrayList[SourceBean] = null
  
  var postProcessingList : ArrayList[PostProcessingBean] = null
  
  /**
   * Get the task id 
   */
  def getTaskName: String = taskName
  
  /**
   * Get the task sql 
   */
  def getTaskSQL: String = taskSQL
  
  /**
   * Get the dataframe name 
   */
  def getDataframeName: String = dataframeName
  
  /**
   * Get the post processing pattern 
   */
  def getPostProcessingPattern: String = postProcessingPattern
  
  /***
   * Get the source list 
   */
  def getSourceList : ArrayList[SourceBean] = sourceList
  
  /**
   * 
   */
  def setSourceList (srcList: ArrayList[SourceBean]) = {
    sourceList = srcList
  }
  
  /***
   * Get the source list 
   */
  def getPostProcessingList : ArrayList[PostProcessingBean] = {
    return postProcessingList
  }
  
  /**
   * 
   */
  def setPostProcessingList (ppList: ArrayList[PostProcessingBean]) = {
    postProcessingList = ppList
  }
  
  /***
   * Override toString
   */
  override def toString: String = {
    s"[TASK_ID: ${taskName}, " +
      s"DATAFRAME_NAME: ${dataframeName}, " +
      s"TASK_SQL: ${taskSQL}, " +
      s"POSTPROCESSING_PATTERN: ${postProcessingPattern}]"
  }
  
}