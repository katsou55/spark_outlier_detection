package outlier.detection

/**
  * Created by carlosrodrigues on 23/01/2017.
  */

import org.apache.spark.sql._
import org.apache.spark.sql.functions._



case class Univariate extends Serializable {


  case class Outlier[T](mean: Double, std: Double,ctx: SQLContext, colName: String) {

    def predict[T](data: Dataset[T],treshold: Int=3) = {
      require(treshold>=2,"treshold invalid")
      val spark = this.ctx.sparkContext
      val leftLimit = spark.broadcast(this.mean-treshold*this.std)
      val rigthLimit = spark.broadcast(this.mean+treshold*this.std)

      import data.sqlContext.implicits._


      data.select(col(colName) ,when(col(colName) > rigthLimit.value || col(colName) < leftLimit.value, "outlier")
                  .otherwise("normal")
                  .as("unvariate_prediction"))
    }


  }

    object Outlier {
      def apply[T](data: Dataset[T], colName: String) = {
        require(data.columns.contains(colName), "Column name not valid")

        val spark = data.sqlContext
        import spark.implicits._

        val stats = data.select(colName).describe()
        val mean = stats.where($"summary" === "mean").first().getString(0).toDouble
        val std = stats.where($"summary" === "stddev").first().getString(0).toDouble
        new Outlier(mean = mean, std = std, spark, colName)

      }

    }




}
