package com.tklake.excelreader

import java.io.File
import java.util.{ArrayList, HashMap}

import com.tklake.bean.{PackageBean, PostProcessingBean, SourceBean, TargetTableBean, TaskBean}
import com.tklake.service.PackageService.addPackage
import com.tklake.service.TargetTableService.addTargetTable
import org.apache.poi.ss.usermodel.{Row, Workbook, WorkbookFactory}

object ExcelTemplateReader {
  val SOURCES_SHEET = "Sources"
  val SOURCES_TYPE = 0
  val SOURCES_PATHSQL = 1
  val SOURCES_DATAFRAME_NAME = 2
  val SOURCES_REMOVE_DUPLICATES = 3
  val SOURCES_TASK_ASSOCIATED_TO = 4

  val TASKS_SHEET = "Tasks"
  val TASKS_TASK_NAME = 0
  val TASKS_SQL = 1
  val TASKS_DATAFRAME_NAME = 2
  val TASKS_POST_PROCESSING = 3


  val POST_PROCESSING_SHEET = "Post Processing"
  val POST_PROCESSING_PATTERN = 0
  val POST_PROCESSING_TYPE = 1
  val POST_PROCESSING_PARAMS = 2

  val PACKAGE_SHEET = "Package"
  val PACKAGE_PACKAGE_NAME = 0
  val PACKAGE_COLUMN_NAME = 1
  val PACKAGE_FORMULA = 2
  val PACKAGE_COLUMN_TYPE = 3


  val TARGET_TABLE_SHEET = "Target Table"
  val TARGET_TABLE_TGT_TBL = 0
  val TARGET_TABLE_TGT_TBL_PARTITION_COLUMN = 1
  val TARGET_TABLE_TGT_TBL_TYPE = 2
  val TARGET_TABLE_TGT_TBL_DATASOURCE = 3
  val TARGET_TABLE_TGT_TBL_MODE = 4

}


/**
  * Reader class which will read the excel and start the magic
  */
class ExcelTemplateReader {

  /**
    * Will read the template from the path and create bean classes
    */
  def readExcel(path: String): ArrayList[TaskBean] = {
    // reading the file 
    val workbook = WorkbookFactory.create(new File(path))

    println("Reading excel ::" + path)

    //Read the source sheet
    val sourceMap = readSources(workbook)
    val postProcessing = readPostProcessing(workbook)
    readPackageSheet(workbook)
    readTargetTableSheet(workbook)
    val tasks = readTasks(workbook, sourceMap, postProcessing)

    return tasks
  }

  /** *
    * Read the sources sheet in the workbook
    */
  def readSources(workbook: Workbook): HashMap[String, ArrayList[SourceBean]] = {
    val sheet = workbook.getSheet(ExcelTemplateReader.SOURCES_SHEET)

    if (sheet == null) {
      println("Sources sheet does not exist. Kindly check your template.")
    }

    println("START: Reading the source sheet in the template")
    val sourceMap = new HashMap[String, ArrayList[SourceBean]]
    var sourceList = new ArrayList[SourceBean]
    val rowIterator = sheet.rowIterator()
    //Skip the header row 
    rowIterator.next()
    while (rowIterator.hasNext()) {
      val row = rowIterator.next()
      val taskName = getCellValue(row, ExcelTemplateReader.SOURCES_TASK_ASSOCIATED_TO)
      sourceList = sourceMap.getOrDefault(taskName, new ArrayList[SourceBean])
      sourceList.add(new SourceBean(getCellValue(row, ExcelTemplateReader.SOURCES_TYPE),
        getCellValue(row, ExcelTemplateReader.SOURCES_PATHSQL),
        getCellValue(row, ExcelTemplateReader.SOURCES_DATAFRAME_NAME),
        getCellValue(row, ExcelTemplateReader.SOURCES_REMOVE_DUPLICATES)))
      sourceMap.put(taskName, sourceList)
    }
    println("END: Reading the source sheet in the template")

    return sourceMap
  }

  /** *
    * Read the Tasks sheet
    */
  def readTasks(workbook: Workbook,
                sourceMap: HashMap[String, ArrayList[SourceBean]],
                postProcessing: HashMap[String, ArrayList[PostProcessingBean]])
  : ArrayList[TaskBean] = {
    val sheet = workbook.getSheet(ExcelTemplateReader.TASKS_SHEET)

    if (sheet == null) {
      println("Tasks sheet does not exist. Kindly check your template.")
    }

    println("START: Reading the tasks sheet in the template")
    val taskList = new ArrayList[TaskBean]
    val rowIterator = sheet.rowIterator()
    //Skip the header row 
    rowIterator.next()
    while (rowIterator.hasNext()) {
      val row = rowIterator.next()
      val taskBean = new TaskBean(getCellValue(row, ExcelTemplateReader.TASKS_TASK_NAME),
        getCellValue(row, ExcelTemplateReader.TASKS_SQL),
        getCellValue(row, ExcelTemplateReader.TASKS_DATAFRAME_NAME),
        getCellValue(row, ExcelTemplateReader.TASKS_POST_PROCESSING))

      println(taskBean.getPostProcessingPattern)
      taskBean.setSourceList(sourceMap.getOrDefault(
        taskBean.getTaskName, new ArrayList[SourceBean]))
      taskBean.setPostProcessingList(
        postProcessing.getOrDefault(taskBean.getPostProcessingPattern,
          new ArrayList[PostProcessingBean]))
      taskList.add(taskBean)
    }
    println("END: Reading the tasks sheet in the template")

    return taskList
  }

  /** *
    * Read the Post Processing sheet
    */
  def readPostProcessing(workbook: Workbook)
  : HashMap[String, ArrayList[PostProcessingBean]] = {
    val sheet = workbook.getSheet(ExcelTemplateReader.POST_PROCESSING_SHEET)

    if (sheet == null) {
      println("Post Processing sheet does not exist. Kindly check your template.")
    }

    println("START: Reading the Post Processing sheet in the template")
    val postProcessingMap = new HashMap[String, ArrayList[PostProcessingBean]]
    val rowIterator = sheet.rowIterator()
    //Skip the header row 
    rowIterator.next()
    while (rowIterator.hasNext()) {
      val row = rowIterator.next()
      val pattern = getCellValue(row, ExcelTemplateReader.POST_PROCESSING_PATTERN)
      val postProcessingBean = new PostProcessingBean(
        getCellValue(row, ExcelTemplateReader.POST_PROCESSING_PATTERN),
        getCellValue(row, ExcelTemplateReader.POST_PROCESSING_TYPE),
        getCellValue(row, ExcelTemplateReader.POST_PROCESSING_PARAMS))

      val postProcessingList = postProcessingMap.getOrDefault(pattern,
        new ArrayList[PostProcessingBean])
      postProcessingList.add(postProcessingBean)

      postProcessingMap.put(pattern, postProcessingList)
    }
    println("END: Reading the Post Processing sheet in the template")
    return postProcessingMap
  }


  /**
    * Read the package sheet
    *
    * @param workbook
    */
  def readPackageSheet(workbook: Workbook) = {
    val sheet = workbook.getSheet(ExcelTemplateReader.PACKAGE_SHEET)

    if (sheet == null) {
      println("Package sheet does not exist. Kindly check your template.")
    }

    println("START: Reading the Package sheet in the template")
    val rowIterator = sheet.rowIterator()
    //Skip the header row
    rowIterator.next()
    while (rowIterator.hasNext()) {
      val row = rowIterator.next()
      val packageName = getCellValue(row, ExcelTemplateReader.PACKAGE_PACKAGE_NAME)
      val packageBean = new PackageBean(
        getCellValue(row, ExcelTemplateReader.PACKAGE_COLUMN_NAME),
        getCellValue(row, ExcelTemplateReader.PACKAGE_FORMULA),
        getCellValue(row, ExcelTemplateReader.PACKAGE_COLUMN_TYPE))

      addPackage(packageName, packageBean)
    }
    println("END: Reading the Package sheet in the template")
  }


  /**
    * Read the package sheet
    *
    * @param workbook
    */
  def readTargetTableSheet(workbook: Workbook) = {
    val sheet = workbook.getSheet(ExcelTemplateReader.TARGET_TABLE_SHEET)

    if (sheet == null) {
      println("Target Table sheet does not exist. Kindly check your template.")
    }

    println("START: Reading the Target Table sheet in the template")
    val rowIterator = sheet.rowIterator()
    //Skip the header row
    rowIterator.next()
    while (rowIterator.hasNext()) {
      val row = rowIterator.next()
      val tgtTableName = getCellValue(row, ExcelTemplateReader.TARGET_TABLE_TGT_TBL)
      val tgtTableBean = new TargetTableBean(
        getCellValue(row, ExcelTemplateReader.TARGET_TABLE_TGT_TBL),
        getCellValue(row, ExcelTemplateReader.TARGET_TABLE_TGT_TBL_PARTITION_COLUMN),
        getCellValue(row, ExcelTemplateReader.TARGET_TABLE_TGT_TBL_TYPE),
        getCellValue(row, ExcelTemplateReader.TARGET_TABLE_TGT_TBL_DATASOURCE),
        getCellValue(row, ExcelTemplateReader.TARGET_TABLE_TGT_TBL_MODE))

      addTargetTable(tgtTableName, tgtTableBean)
    }
    println("END: Reading the Target Table sheet in the template")
  }


  /** *
    * Get the string value from the cell
    */
  def getCellValue(row: Row, cellIndex: Int): String = {
    if (row.getCell(cellIndex) == null) {
      return null
    }
    return row.getCell(cellIndex).getStringCellValue
  }
}