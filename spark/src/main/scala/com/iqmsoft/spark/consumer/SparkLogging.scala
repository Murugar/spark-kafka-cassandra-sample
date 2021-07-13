package com.iqmsoft.spark.consumer

import org.apache.log4j.{Level, Logger}

import org.apache.spark.Logging


object SparkLogging extends Logging {

  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for this app.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}