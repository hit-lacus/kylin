/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.metrics

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

object MetricsEventTool extends Logging {

  // SparkListenerJobStart
  // SparkListenerJobEnd


  // SparkListenerStageSubmitted
  // SparkListenerStageCompleted

  // SparkListenerTaskEnd

  // SparkListenerSQLExecutionStart
  // SparkListenerSQLExecutionEnd

  // SparkListenerExecutorMetricsUpdate
  // SparkListenerDriverAccumUpdates

  def print(event: SparkListenerEvent): Unit = {

    event match {
      case jobEvent: SparkListenerJobStart => {
        logInfo("Job " + jobEvent.jobId + " is started.")
      }
      case jobEvent: SparkListenerJobEnd => {
        logInfo("Job " + jobEvent.jobId + " is end, " + jobEvent.jobResult)
      }

      case stageEvent: SparkListenerStageSubmitted => {
        logInfo("Stage " + stageEvent.stageInfo.stageId + "~ " + stageEvent.stageInfo.name + "~ "
          + stageEvent.stageInfo.details + "~ " + stageEvent.properties.stringPropertyNames().toArray.mkString("/") + " is started.")

        stageEvent.properties.list(System.out)
      }

      case stageEvent: SparkListenerStageCompleted => {
        logInfo("Stage " + stageEvent.stageInfo.stageId + ", " + stageEvent.stageInfo.name + ", "
          + stageEvent.stageInfo.details + " is completed.")
        logInfo("All task returned " + stageEvent.stageInfo.taskMetrics.resultSize + " records .")
      }

      case queryEvent: SparkListenerSQLExecutionStart => {
        val nodeName = queryEvent.sparkPlanInfo.nodeName
        val simpleString = queryEvent.sparkPlanInfo.simpleString
        val metadataString = queryEvent.sparkPlanInfo.metadata.mkString(", ")
        val metricsString = queryEvent.sparkPlanInfo.metrics.mkString(",")
        logInfo("Query " + queryEvent.executionId + " is starting, desc is " + queryEvent.description
          + ", and detail is " + queryEvent.details + " , physicalPlanDescription is " + queryEvent.physicalPlanDescription + " .")
        logInfo("Query " + queryEvent.executionId + ", nodeName is " + nodeName + ", simpleString is " + simpleString
          + ", metadataString is " + metadataString + ", metricsString is " + metricsString)
        logInfo("All task returned " + queryEvent + " records .")
      }

      case queryEvent: SparkListenerSQLExecutionEnd => {
        logInfo("Query " + queryEvent.executionId + " is completed at " + queryEvent.time)
      }


      case _ => logInfo("Unknown " + event.getClass + ".")
    }

  }

//  def main(args: Array[String]): Unit = {
//    val m = Map("1" -> "aa1", "2" -> "aa2", "3" -> "aa3")
//    println(m.mkString(","))
//  }
}
