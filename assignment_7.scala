// Required imports
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._
import org.apache.spark.mllib.clustering._

val file_open = sc.textFile("/home/harry/Documents/scala_files/input/adult.data")
val raw_data = file_open.map(x => x.split(", "))
val data = raw_data.map{x => 
	(if(x(9) == "Male") {1.0} else if(x(9) == "Female") {2.0} else {3.0},
	if(x(13) == "United-States") {1.0} else if(x(13) == "Canada") {2.0} else {3.0},
	if(x(14) == "<=50K") {1.0} else if(x(14) == ">50K") {2.0} else {3.0})}
val dense_data = data.map{x => 
	Vectors.dense(x._1, x._2, x._3)}
val k_means = new KMeans()
k_means.setK(2)
val model = k_means.run(dense_data)
model.clusterCenters.foreach(println)
val test_data = model.predict(Vectors.dense(1.0,1.0,2.0))
