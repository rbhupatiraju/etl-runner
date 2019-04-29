package com.tklake.service

import com.tklake.App.sqlContext
import com.tklake.service.PackageService.getPackageDetails
import org.apache.spark.sql.DataFrame

object PackageExecutionService {

  /**
    * Execute the package to add additional columns to the existing DataFrame
    *
    * @param currDf
    * @param packageName
    * @return
    */
  def executePackage(currDf: DataFrame, packageName: String)
  : DataFrame = {
    val pkgList = getPackageDetails(packageName)
    val listIterator = pkgList.listIterator
    var nextDf = currDf

    while (listIterator.hasNext) {
      val pkgBean = listIterator.next

      // Register the temp view
      nextDf.createOrReplaceTempView(s"tmp_${pkgBean.getColumnName}")

      val tmpTableQuery = s"select s.*, " +
        s"${pkgBean.getFormula} as ${pkgBean.getColumnName} " +
        s"from tmp_${pkgBean.getColumnName} s"

      // Adding the new column to the existing data frame
      nextDf = sqlContext.sql(tmpTableQuery)

      // Dropping the table
      sqlContext.dropTempTable(s"tmp_${pkgBean.getColumnName}")
    }

    return nextDf
  }

}
