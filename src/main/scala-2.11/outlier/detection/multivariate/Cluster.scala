package outlier.detection.multivariate

/**
  * Created by carlosrodrigues on 25/01/2017.
  */

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset}


/**
  *
  **/
case class VecData[T](featureCols: Array[String], data: Dataset[T]) extends Serializable {

  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")

  def transform[T](data: Dataset[T]) = {
    assembler.transform(data)
  }

}

/**
  *
  **/
case class Scaler[T]() extends Serializable {

  def standard(data: Dataset[T]) = {
    import org.apache.spark.ml.feature.StandardScaler
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true) //false by deafult
    // Compute summary statistics by fitting the StandardScaler.
    // Normalize each feature to have unit standard deviation.
    scaler.fit(data).transform(data)
  }

  def normal(data: Dataset[T]) = {
    import org.apache.spark.ml.feature.Normalizer
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setP(1.0)
    normalizer.transform(data)
  }

  def maxMin(data: Dataset[T]) = {
    import org.apache.spark.ml.feature.MinMaxScaler
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    scaler.fit(data).transform(data)
  }

  def maxAbs(data: Dataset[T]) = {
    import org.apache.spark.ml.feature.MaxAbsScaler
    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    scaler.fit(data).transform(data)
  }

}

/**
  *
  **/
case class Cluster[T](featureCols: Array[String], scaling: String, cluster: (Int, Int, Int, String)) extends Serializable {
  //[T](data: Dataset[T], scaling: Option[String]) {


  def predict[T](data: Dataset[T]) = {

    val vec = VecData(featureCols, data).transform(data)

    val df = scaling match {
      case "None" => vec.select(vec("features") as "scaledFeatures")
      case "Standardization" => Scaler().standard(vec)
      case "Normalization" => Scaler().normal(data)
      case "MaxMin" => Scaler().maxMin(data)
      case "MaxAbs" => Scaler().maxAbs(data)

    }

    df.show(5)
    val outData = scaling match {
      case "None" => df.select(df("scaledFeatures") as "features")
      case _ => df.drop("features").select(df("scaledFeatures") as "features")
    }

    clusterMethod(outData)

  }

  def clusterMethod(data: DataFrame): DataFrame = {
    val k = this.cluster._1
    val maxIte = this.cluster._2
    val seed = this.cluster._3
    val c = this.cluster._4

    val model: DataFrame = c match {
      case "GMM" => GMM(data, k, maxIte, seed)
      case "K-means" => Kmeans(data, k, maxIte, seed)
      case _ => Kmeans(data, k, maxIte, seed)
        //"Invalid clustering algorithm name (K-means or GMM). Using defaul K-means"
    }

    model
  }

  def GMM(data: DataFrame, k: Int, maxIte: Int, seed: Int): DataFrame = {
    import org.apache.spark.ml.clustering.GaussianMixture
    val gmm = new GaussianMixture()
      .setK(k)
      .setMaxIter(maxIte)
      .setSeed(seed)

    data.cache()
    val model = gmm.fit(data).transform(data)
    data.unpersist()
    model
  }

  def Kmeans(data: DataFrame, k: Int, maxIte: Int, seed: Int): DataFrame = {
    import org.apache.spark.ml.clustering.KMeans
    val kmeans = new KMeans()
      .setK(k)
      .setMaxIter(maxIte)
      .setSeed(seed)

    data.cache()
    val model = kmeans.fit(data).transform(data)
    data.unpersist()
    model
  }


}

/**
  * Companion Object
  * */
object Cluster extends Serializable {
  def apply(featureCols: Array[String], k: Int = 2, maxIte: Int = 10, seed: Int = 2017, method: String = "K-means", scaling: String = "None") = {

    //new Cluster(k = k, maxIte = maxIte, seed = seed, method = method)
    val cluster = (k, maxIte, seed, method)
    new Cluster(featureCols = featureCols, scaling = scaling, cluster = cluster)
  }

}