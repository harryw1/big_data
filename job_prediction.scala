import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.rdd._

// read in the file and prepare the data
var input_file = sc.textFile("/home/harry/Documents/scala_files/input/job_prediction.txt");
// if the file has a header, we want to ignore it, but in this case, i did not have a header
val header = input_file.first();
val raw_data = input_file.filter(x => x != header);
val data = raw_data.map{x =>
    val values = x.split(',').map{x => x.toDouble}
    val feature_vectors = Vectors.dense(values.init) // init gets everything but the last value)
    val label = values.last
    LabeledPoint(label, feature_vectors)};

// training
val categorical_info = Map[Int,Int]()
// dataset, decisions to make, working set, algorithm to use, depth of deicison tree, how many trees we are going to consider
val model = DecisionTree.trainClassifier(data, 3, categorical_info, "gini", 7, 100);

// test
val test_data_1 = Vectors.dense(25, 1, 3, 0, 0, 1);
val prediction = model.predict(test_data_1);

val test_data_2 = Vectors.dense(1, 0, 0, 2, 1, 1);
val prediction = model.predict(test_data_2);

val test_data_3 = Vectors.dense(10, 0, 2, 1, 0, 1);
val prediction = model.predict(test_data_3);

val test_data_4 = Vectors.dense(0, 0, 0, 0, 1, 1);
val prediction = model.predict(test_data_4);

val test_data_5 = Vectors.dense(2, 0, 1, 0, 0, 0);
val prediction = model.predict(test_data_5);

// println("Model Tree: \n" + model.toDebugString);