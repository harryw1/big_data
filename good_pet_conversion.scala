import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._

// read in the file and prepare the data
var input_file = sc.textFile("/home/harry/scala_files/input/good_pet_conversion.txt");
val raw_data = input_file.map(x => x.split(','))
val data = raw_data.map{x =>
    ((x(1).toDouble,
    x(2).toInt,
    if (x(3) == "Brown") {0} else if (x(3) == "Green") {1} else if (x(3) == "Tan") {2} else if (x(3) == "Grey") {3} else {4},
    if (x(4) == "Yes") {1} else {0}))}

val parsed_data = data.map{x => 
	val featureVector = Vectors.dense(x._1, x._2, x._3)
	val label = x._4
	LabeledPoint(label, featureVector)
	}

// training
val categorical_info = Map[Int,Int]()
// dataset, decisions to make, working set, algorithm to use, depth of deicison tree, how many trees we are going to consider
val model = DecisionTree.trainClassifier(data, 2, categorical_info, "gini", 4, 100);

// test
val test_data_1 = Vectors.dense(15,2,0)
val prediction = model.predict(test_data_1);

println("Model Tree: \n" + model.toDebugString);
