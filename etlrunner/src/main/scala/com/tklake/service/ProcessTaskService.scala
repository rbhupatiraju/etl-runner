package com.tklake.service

import java.util.ArrayList

import com.tklake.App.sqlContext
import com.tklake.bean.TaskBean
import com.tklake.service.DataReadService.getSourceMap
import com.tklake.service.TableRegistrationService.{registerTempTables, unregisterTempTables}
import com.tklake.service.PostProcessingService.executePostProcessingSteps
import org.apache.spark.sql.DataFrame

class ProcessTaskService {


  /**
    * Process the list of tasks defined by the user
    *
    * @param taskList
    */
  def processTaskList(taskList: ArrayList[TaskBean]) = {
    val listIterator = taskList.iterator()

    val dfList = new ArrayList[String]

    while (listIterator.hasNext()) {
      val taskBean = listIterator.next()

      // Process the task
      var currDf = processTask(taskBean)

      //Executing post processing steps for the task
      currDf = executePostProcessingSteps(currDf, taskBean.getPostProcessingList)

      //Register the output datafrom as a temp table for use in the next step
      currDf.createOrReplaceTempView(taskBean.getDataframeName)

      currDf.show

      dfList.add(taskBean.getDataframeName)
    }

    // Cleanup previously created temp views
    cleanupCachedDataFrames(dfList)
  }

  /**
    * Processes the task and returns the task bean
    *
    * @param taskBean
    * @return
    */
  def processTask(taskBean: TaskBean)
  : DataFrame = {
    // Get the list of sources
    val sourceMap = getSourceMap(taskBean.getSourceList)

    //Register the sources as temp tables
    registerTempTables(sourceMap)

    // Get the dataframe from the sql
    val df = sqlContext.sql(taskBean.getTaskSQL)

    //Unregister the sources to save memory
    unregisterTempTables(sourceMap)

    return df
  }

  /**
    * Clean up the dataframes cached during the ETL Job Run
    *
    * @param dfList
    */
  def cleanupCachedDataFrames(dfList: ArrayList[String]) = {
    val dfListIterator = dfList.listIterator()
    while (dfListIterator.hasNext) {
      val dfName = dfListIterator.next
      sqlContext.dropTempTable(dfName)
    }
  }
}
