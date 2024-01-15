/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

ThisBuild / version := "0.1.0"

// ThisBuild / scalaVersion := sys.env.getOrElse("scalaVersion", "2.13.12")
// val sparkVersion = sys.env.getOrElse("sparkVersion", "3.5.0")
ThisBuild / scalaVersion := "2.13.12"
val sparkVersion = "3.5.0"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "-" + module.revision + "-" + sparkVersion.self + "_" +
    scalaBinaryVersion.value + "." + artifact.extension
}

libraryDependencies += "org.apache.spark" %% s"spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% s"spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% s"spark-catalyst" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% s"spark-hive" % sparkVersion % "provided"

libraryDependencies += "org.scalatest" %% s"scalatest" % "3.2.3" % Test
libraryDependencies += "org.scalacheck" %% s"scalacheck" % "1.14.2" % Test
libraryDependencies += "junit" % "junit" % "4.13.2" % Test

Test / fork := true
Test / parallelExecution := true

