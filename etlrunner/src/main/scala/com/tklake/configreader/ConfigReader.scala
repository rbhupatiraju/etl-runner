package com.tklake.configreader

import java.io.{BufferedReader, FileReader}

import com.tklake.bean.DataSourceBean
import com.tklake.util.DataSourceUtil.addDataSource
import org.apache.commons.lang.StringUtils

object ConfigReader {

  /** *
    * Load common utility database properties
    */
  def loadDatabaseProperties: Unit = {
    val br = new BufferedReader(new FileReader("../conf/database.properties"))
    var line: String = br.readLine()
    var dsName, url, driver, username, password: String = null
    while (line != null) {
      if (StringUtils.isEmpty(line)) {
        println(s"s[URL: ${url}, DRIVER: ${driver}, USERNAME: ${username}, PASSWORD: ${password}]")
        addDataSource(dsName, new DataSourceBean(driver, url, username, password))
      } else if (line.indexOf("=") <  0) {
        dsName = line.trim
      } else if (line.indexOf("=") > 0) {
        val attribute = line.substring(0, line.indexOf("="))
        val attributeValue = line.substring(line.indexOf("=")+1)
        attribute match {
          case "username" => username = attributeValue
          case "password" => password = attributeValue
          case "url" => url = attributeValue
          case "driver" => driver = attributeValue
        }
      }
      line = br.readLine()
    }
    addDataSource(dsName, new DataSourceBean(driver, url, username, password))
  }
}
