/**
  * Created by carlosrodrigues on 23/01/2017.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import oulier.detection.univariate._


object Main {

  def main(args: Array[String]) {

    //Initiazlize sparkSession as the unified context
    val spark = SparkSession
      .builder()
      .appName("OutlierDetection")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    import spark.implicits._

    /**       ----       ----      ----       */
    /**       ----       BODY       ----       */
    /**       ----       ----       ----       */

    val filepath = "/Users/carlosrodrigues/Downloads/MHEALTHDATASET/mH*"


    val allSignalsRDD = spark.sparkContext.wholeTextFiles(filepath).mapValues(_.split("\n"))

    def getSubjectNumber(s:String):String = {
      val pattern = "subject[0-9]*".r
      val year = pattern.findFirstIn(s).getOrElse("null")
      year.replace("subject", "")
    }

    val allSignalsDF = allSignalsRDD
      .flatMapValues(x => x)
      .mapValues(_.split("\n"))
      .map(x => x._2.mkString("\t") + "\t" + getSubjectNumber(x._1) )
      .map(_.split("\t"))
      .map(x => DeviceIoTData(x(0).toDouble,x(1).toDouble,x(2).toDouble,x(3).toDouble,
        x(4).toDouble,x(5).toDouble,x(6).toDouble,x(7).toDouble,x(8).toDouble,
        x(9).toDouble,x(10).toDouble,x(11).toDouble,x(12).toDouble,x(13).toDouble,x(14).toDouble,
        x(15).toDouble,x(16).toDouble,x(17).toDouble,x(18).toDouble,x(19).toDouble,x(20).toDouble,
        x(21).toDouble,x(22).toDouble,x(23).toDouble,x(24).toDouble))
      .toDS()

    allSignalsDF.cache
    val nlines = allSignalsDF.count

    println(s"The number of lines is: ${nlines}")

    allSignalsDF.show(2)

    val outlier = Outlier(allSignalsDF, "ECG_1")
    outlier.predict(allSignalsDF).groupBy($"unvariate_prediction").agg(count($"unvariate_prediction")).show(5)


    spark.stop()
  }

  case class DeviceIoTData (acc_chest_x: Double,
                            acc_chest_y: Double,
                            acc_chest_z: Double,
                            ECG_1: Double,
                            ECG_2: Double,
                            acc_L_anckle_x: Double,
                            acc_L_anckle_y: Double,
                            acc_L_anckle_z: Double,
                            gyro_L_anckle_x: Double,
                            gyro_L_anckle_y: Double,
                            gyro_L_anckle_z: Double,
                            mag_L_anckle_x: Double,
                            mag_L_anckle_y: Double,
                            mag_L_anckle_z: Double,
                            acc_R_lowerArm_x: Double,
                            acc_R_lowerArm_y: Double,
                            acc_R_lowerArm_z: Double,
                            gyro_R_lowerArm_x: Double,
                            gyro_R_lowerArm_y: Double,
                            gyro_R_lowerArm_z: Double,
                            mag_R_lowerArm_x: Double,
                            mag_R_lowerArm_y: Double,
                            mag_R_lowerArm_z: Double,
                            Label: Double,
                            ID: Double) extends Serializable

}

