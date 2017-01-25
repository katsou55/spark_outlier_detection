name := "outlier_detection_spark"

version := "1.0"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark".%("spark-sql_2.11") % "2.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.11
libraryDependencies += "org.apache.spark".%("spark-mllib_2.11") % "2.1.0"

// https://mvnrepository.com/artifact/org.scalaz/scalaz-core_2.11
//libraryDependencies += "org.scalaz" % "scalaz-core_2.11" % "7.2.0-RC1"
