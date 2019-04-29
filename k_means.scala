// Required imports
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._
import org.apache.spark.mllib.clustering._

// K-Means
val file_open = sc.textFile("/home/harry/Documents/scala_files/input/driver_data.csv")
val header = file_open.first()
val raw_data = file_open.filter(x => x != header).map(x => x.split(','))
// creating a tuple of distance traveled and speed
val data = raw_data.map{x => (x(1).toDouble, x(2).toDouble)}
// to continue using the data, we need to create a dense vector from the tuple
val dense_data = data.map{x => Vectors.dense(x._1, x._2)}

// printing our dense vector
// dense_data.collect.foreach(println)

val k_means = new KMeans()
// we set the number of k to two, becasue there are at least
// two clusters in this dataset
k_means.setK(4)
val model = k_means.run(dense_data)

// printing the predictions for our training data
model.clusterCenters.foreach(println)

val test_data = model.predict(Vectors.dense(200,100))
val test_data1 = model.predict(Vectors.dense(25,50))
val test_data2 = model.predict(Vectors.dense(400,25))
val test_data3 = model.predict(Vectors.dense(49,5))
