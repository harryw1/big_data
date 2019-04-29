// import org.apache.spark.mllib.linalg._
// import org.apache.spark.mllib.regression._
// import org.apache.spark.mllib.evaluation._
// import org.apache.spark.mllib.tree._
// import org.apache.spark.mllib.tree.model._
// import org.apache.spark.mllib.rdd._

// // read in the file and prepare the data
// var input_file = sc.textFile("/home/harry/Documents/scala_files/input/job_prediction_conversion.txt");
// // if the file has a header, we want to ignore it
// val header = input_file.first();
// val raw_data = input_file.filter(x => x != header);

// // processing the categorical data and converting it to numerical data

// val processed_data = raw_data.map(x => x.split(','))
// val data = processed_data.map(x => 
//     (if (x(0) == "?") {-1} else {x(0).toDouble},
//     if(x(1) == "Y") {1} else {0}, if(x(2) == "?") {-1} else {x(2).toDouble}, 
//     if(x(3) == "BS") {1} else if (x(3) == "MS") {2} else if (x(3) == "PhD") {3} else {4}, 
//     if(x(4) == "Y") {1} else {0},
//     if(x(5) == "Y") {1} else {0},
//     if(x(6) == "Y") {1} else {0}))

//     // (if(x(0) == '?') {-1} else {x(0).toDouble},
//     // if(x(1) == 'Y') {1} else if(x(1) == 'N') {0} else {-1},
//     // if(x(2) == '?') {-1} else {x(2).toDouble},
//     // if(x(3) == "BS") {1} else if(x(3) == "MS") {2} else if(x(3) == "PhD") {3} else {-1},
//     // if(x(4) == 'Y') {1} else if(x(4) == 'N') {0} else {-1},
//     // if(x(5) == 'Y') {1} else if(x(5) == 'N') {0} else {-1},
//     // if(x(6) == 'Y') {1} else if(x(6) == 'N') {0} else {-1}))

// val training_data = data.map{x => 
// 	val featureVector = Vectors.dense(x._1, x._2, x._3, x._4, x._5, x._6)
// 	val label = x._7
// 	println(label)
// 	println(featureVector)
// 	LabeledPoint(label, featureVector)
// 	}

// // here we are saying at certain attributes, the number of categories that exist
// // example: for the third attribute, we have 4 possible categories, but feature info starts from 0, so there are 5 categories
// val categorical_feature_info = Map[Int, Int] ((1,2), (3,5), (4,2), (5,2))
// // the first argument is the dataset, the second argument is how many categories are in the label (the thing to be predicted),
// // the third argument is our categorical feature info, the fourth argument is the algorithm, the fifth argument is the depth of the tree (1/2 total attriutes plus 1),
// // the sixth argument is number of trees to make
// val model = DecisionTree.trainClassifier(data, 2, categorical_feature_info, "gini", 5, 100)

// val test_data = Vectors.dense(10,1,3,1,0,0)
// val prediction = model.predict(test_data)

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._

val file = sc.textFile ("/home/harry/Documents/scala_files/input/job_prediction_conversion.txt")
val header = file.first()
val rawData = file.filter(x=> x != header )
val fileData = rawData.map(x => x.split(","))
val parsedData = fileData.map( x => (if (x(0) == "?") {-1} else {x(0).toDouble},if(x(1) == "Y") {1} else {0}, if(x(2) == "?") {-1} else {x(2).toDouble}, if(x(3) == "BS") {1} else if (x(3) == "MS") {2} else if (x(3) == "PhD") {3} else {4}, if(x(4) == "Y") {1} else {0}, if(x(5) == "Y") {1} else {0}, if(x(6) == "Y") {1} else {0}))

val data = parsedData.map{x => 
	val featureVector = Vectors.dense(x._1, x._2, x._3, x._4, x._5, x._6)
	val label = x._7
	println(label)
	println(featureVector)
	LabeledPoint(label, featureVector)
	}

val categoricalFeatureInfo = Map[Int, Int] ((1,2), (3,5), (4,2), (5,2) )
val model = DecisionTree.trainClassifier (data, 2, categoricalFeatureInfo, "gini", 5, 100)

val testData = Vectors.dense (0, 0, 0, 1, 0, 0)//0,N,0,BS,N,N,N
val prediction = model.predict(testData)
println("Learned Classification tree model: \n" + model.toDebugString)