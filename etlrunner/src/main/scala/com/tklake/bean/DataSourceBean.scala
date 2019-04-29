package com.tklake.bean

class DataSourceBean (driver: String,
                      url: String,
                      username: String,
                      password: String) {

  /**
    * Get the driver of the datasource
    * @return
    */
  def getDriver : String = driver

  /**
    * Get the URL of the datasource
    *
    * @return
    */
  def getUrl : String = url

  /**
    * Get the username of the datasource
    * @return
    */
  def getUsername : String = username

  /**
    * Get the password of the datasource
    * @return
    */
  def getPassword : String = password

  /**
    * Override the toString method
    * @return
    */
  override def toString : String = {
    s"[DRIVER: ${driver}, URL: ${url}, USERNAME: ${username}]"
  }
}
