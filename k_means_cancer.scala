// Required imports
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._
import org.apache.spark.mllib.clustering._

// K-Means
val file_open = sc.textFile("/home/harry/Documents/scala_files/input/breast-cancer-wisconsin.data")
val raw_data = file_open.map(x => x.split(','))
val data = raw_data.map{x => 
	(if (x(1) == "?") {-1} else {x(1).toDouble},
	if (x(2) == "?") {-1} else {x(2).toDouble},
	if (x(3) == "?") {-1} else {x(3).toDouble},
	if (x(4) == "?") {-1} else {x(4).toDouble},
	if (x(5) == "?") {-1} else {x(5).toDouble},
	if (x(6) == "?") {-1} else {x(6).toDouble},
	if (x(7) == "?") {-1} else {x(7).toDouble},
	if (x(8) == "?") {-1} else {x(8).toDouble},
	if (x(9) == "?") {-1} else {x(9).toDouble})}
// to continue using the data, we need to create a dense vector from the tuple
val dense_data = data.map{x => Vectors.dense(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9)}
val k_means = new KMeans()
k_means.setK(2)
val model = k_means.run(dense_data)
model.clusterCenters.foreach(println)

val test_data = model.predict(Vectors.dense(10,10,10,10,10,10,10,10,10))
val test_data = model.predict(Vectors.dense(0,0,0,0,0,0,0,0,0))
