// Name: Harrison Weiss
// File: Project 2

val file_open = sc.textFile("/home/harry/Documents/scala_files/input/UNSW-NB15_1.csv")
val raw_data = file_open.map(x => x.split(","))
// The key is the timestamp. The values become the IP, and the two timings
val data = raw_data.map{x =>
	((if (x(28) == "?") {-1} else {x(28)}),
	(if (x(0) == "?") {-1} else {x(0)},
	if (x(30) == "?") {-1} else {x(30).toDouble},
	if (x(31) == "?") {-1} else {x(31).toDouble}))}
val reduced_data = data.reduceByKey{case (a,b) => 
	(if (b._2 > b._3) {(b._1, b._2, 0)} else {(b._1, b._3, 0)})}
// Cuts off the trailing zeros
val trimmed_data = reduced_data.mapValues(x => (x._1, x._2))
trimmed_data.collect.foreach(println)
