name := "outlier_detection_spark"

version := "1.0"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark".%("spark-sql_2.11") % "2.1.0"

libraryDependencies += "log4j" % "log4j" % "1.2.17"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
    