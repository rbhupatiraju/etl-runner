package com.tklake.service

import java.util.ArrayList
import java.util.HashMap

import com.tklake.bean.PackageBean

object PackageService {

  var packageMap: HashMap[String, ArrayList[PackageBean]] = new HashMap[String, ArrayList[PackageBean]]

  /**
    * Set the package map
    *
    * @param pkgMap
    */
  def setPackageMap(pkgMap : HashMap[String, ArrayList[PackageBean]])
  = {
    packageMap = pkgMap
  }

  /**
    * Add package bean to the package map
    *
    * @param pkgName
    * @param packageDetails
    */
  def addPackage(pkgName: String, packageDetails: PackageBean)= {
    val pkgList = packageMap.getOrDefault(pkgName, new ArrayList[PackageBean])
    pkgList.add(packageDetails)
    packageMap.put(pkgName, pkgList)
  }

  /**
    * Get the list of package bean
    *
    * @param packageName
    * @return pkgList ArrayList of PackageBean
    */
  def getPackageDetails(packageName: String)
  : ArrayList[PackageBean] = {
    packageMap.getOrDefault(packageName, new ArrayList[PackageBean])
  }
}
