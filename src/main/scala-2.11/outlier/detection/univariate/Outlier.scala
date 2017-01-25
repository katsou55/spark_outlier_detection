package outlier.detection.univariate

/**
  * Created by carlosrodrigues on 23/01/2017.
  *
  * Future work: apply the univariate method to all dataset features which are continues variables in an automatic way
  */

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object imp {
  implicit object DatasetMarker
  implicit object DataframeMarker
}


case class Outlier(mean: Double, std: Double, ctx: SQLContext, colName: String, var outputCol: String, threshold: Int) extends Serializable {

  /**
    * Predict used in DataSet Class of spark.sql
    * */
  def predict[T](data: Dataset[T])(implicit s: imp.DatasetMarker.type) = {
    val limits = broadcastLimits(this.threshold)

    data.select(col(colName), when(col(colName) > limits._2.value || col(colName) < limits._1.value, "outlier")
      .otherwise("normal")
      .as(outputCol))
  }

  /**
    * Predict used in DataFrame Class of spark.sql
    * */
  def predict(data: DataFrame)(implicit f: imp.DataframeMarker.type) = {
    val limits = broadcastLimits(this.threshold)

    data.select(col(colName), when(col(colName) > limits._2.value || col(colName) < limits._1.value, "outlier")
      .otherwise("normal")
      .as(outputCol))
  }

  def broadcastLimits(threshold: Int) ={
    require(threshold >= 2, "threshold invalid")
    val spark = this.ctx.sparkContext
    val leftLimit = spark.broadcast(this.mean - threshold * this.std)
    val rigthLimit = spark.broadcast(this.mean + threshold * this.std)
    (leftLimit,rigthLimit)
  }

  val avg = this.mean

  val stdd = this.std

  def setOutputCol(name: String) = {
    this.outputCol = name
  }

}

object Outlier extends Serializable {

  val verifyThreshold: PartialFunction[Int, Int] = { case t if(2 to 6).contains(t) => t }

  def apply[T](data: Dataset[T], colName: String, threshold: Int)(implicit s: imp.DatasetMarker.type) =  {
    require(data.columns.contains(colName), "Column name not valid")

    val t = verifyThreshold lift threshold match {
      case Some(t) => t
      case None =>
        println("Threshold value Invalid, default value 3 was used.")
        3
    }
    val spark = data.sqlContext
    val stats = data.select (colName).describe()

    get_params (stats, colName, spark, t)
  }

  def apply(data: DataFrame, colName: String, threshold: Int)(implicit f: imp.DataframeMarker.type) = {
    require(data.columns.contains(colName), "Column name not valid")

    val t = verifyThreshold lift threshold match {
      case Some(t) => t
      case None =>
        println("Threshold value Invalid, default value 3 was used.")
        3
    }

    val spark = data.sqlContext
    val stats = data.select (colName).describe()
    get_params (stats, colName, spark, t)
  }

  def get_params(stats: DataFrame, colName: String, spark: SQLContext, threshold: Int) = {

    val std = stats.where(col("summary") === "stddev").first().getString(1).toDouble
    val mean = stats.where(stats("summary") === "mean").first().getString(1).toDouble
    val outputCol = "unvariate_prediction"

    new Outlier(mean = mean, std = std, spark, colName, outputCol, threshold = threshold)
  }
}
