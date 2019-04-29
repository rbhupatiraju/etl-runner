package com.tklake.service

import java.util.ArrayList

import com.tklake.service.DataWriteService.writeToTable
import com.tklake.service.PackageExecutionService.executePackage
import com.tklake.bean.PostProcessingBean
import org.apache.commons.collections.CollectionUtils
import org.apache.spark.sql.DataFrame

object PostProcessingService {

  /***
    * Run post processing steps on task
    *
    * @param postProcessingList
    */
  def executePostProcessingSteps(currDf: DataFrame,
         postProcessingList: ArrayList[PostProcessingBean])
  : DataFrame = {
    // in case there are no post processing steps
    if (CollectionUtils.isEmpty(postProcessingList)) {
      return currDf
    }
    val ppListIterator = postProcessingList.listIterator
    var tgtDf : DataFrame = currDf
    while (ppListIterator.hasNext) {
      val postProcessingBean = ppListIterator.next

      //Execute the post processing step
      tgtDf = executePostProcessingStep(tgtDf, postProcessingBean)
    }
    return tgtDf
  }

  /**
    * Execute the Post Processing Step
    *
    * @param currDf
    * @param ppBean
    */
  def executePostProcessingStep(currDf:  DataFrame,
              ppBean: PostProcessingBean)
  : DataFrame = {
    println(s"Executing ::${ppBean.getProcessingType}")
    val params = ppBean.getProcessingStepParams
    return ppBean.getProcessingType.toLowerCase match {
      case "package" => return executePackage(currDf, params)
      case "table_dump" => return writeToTable(currDf, ppBean.getProcessingStepParams)
      case "drop_duplicates" => return dropDuplicates(currDf, params)
    }
  }

  def dropDuplicates(currDf: DataFrame, colNameString: String)
  : DataFrame = {
    val colNames = colNameString.split(",")
    currDf.dropDuplicates(colNames)
  }
}
