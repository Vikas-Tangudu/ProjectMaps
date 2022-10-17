ThisBuild / version := "V1"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "ProjectMaps",
    libraryDependencies += "org.datasyslab" % "geospark" % "1.3.1",
    libraryDependencies += "org.datasyslab" % "geospark-sql_2.3" % "1.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0",
    libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.27"
  )
