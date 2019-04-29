package com.tklake.bean

class PostProcessingBean(patternName: String,
                         processingType: String,
                         processingStepParams: String) {

  /**
    * Get the Pattern Name
    *
    * @return
    */
  def getPatternName: String = patternName

  /**
    * Get the Processing Type
    *
    * @return
    */
  def getProcessingType: String = processingType

  /**
    * Get the Processing Step Param
    *
    * @return
    */
  def getProcessingStepParams: String = processingStepParams

  /**
    * Overriden toString method
    *
    * @return
    */
  override def toString: String = {
    s"[PATTERN_NAME: ${patternName}, " +
      s"PROCESSING_TYPE: ${processingType}, " +
      s"PROCESSING_STEP_PARAMS: ${processingStepParams}]"
  }
}