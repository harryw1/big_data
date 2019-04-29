// Required imports
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._
import org.apache.spark.mllib.clustering._

val file_open = sc.textFile("/home/harry/Documents/scala_files/input/auto-mpg.data")
// using " +" because it is a regex for all whitespace
val raw_data = file_open.map(x => x.split(" +"))
val data = raw_data.map{x =>
	(if (x(1) == "?") {-1} else {x(1).toDouble},
	if (x(4) == "?") {-1} else {x(4).toDouble},
	if (x(5) == "?") {-1} else {x(5).toDouble})}
val dense_data = data.map{x => 
	Vectors.dense(x._1, x._2, x._3)}
val k_means = new KMeans()
k_means.setK(2)
val model = k_means.run(dense_data)
model.clusterCenters.foreach(println)

val test_data = model.predict(Vectors.dense(6, 4500, 9))
val test_data = model.predict(Vectors.dense(8, 1000, 6)) 
