package com.tklake.bean

class PackageBean (columnName: String,
                   formula: String,
                   columnType: String) {

  /**
    * Get the column name
    *
    * @return columnName
    */
  def getColumnName: String = columnName

  /**
    * Get the formula
    *
    * @return formula
    */
  def getFormula: String = formula

  /**
    * Get the column type
    *
    * @return columnType
    */
  def getColumnType: String = columnType

  /**
    * Overriden toString method
    *
    * @return
    */
  override def toString: String = {
    s"[COLUMN_NAME: ${columnName}, COLUMN_TYPE: ${columnType}, FORMULA: ${formula}]"
  }
}
