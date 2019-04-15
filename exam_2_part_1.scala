// val file = sc.textFile("/home/harry/Documents/scala_files/input/exam_2_1.txt")
// val keyval = file.map(x => x.split(","));
// val result = keyval.map(x => (x(2), x(3), x(25)))
// val diesels = result.filter{case(a,b,c) => b == "diesel"}
// println(diesels.max)

// Solving in class
val file = sc.textFile("/home/harry/Documents/scala_files/input/exam_2_1.txt")
val split = file.map(x => x.split(","));
    // fuel type is key while make and price are the values
val keyval = split.map(x => (x(3), (x(2), x(25))))
    // want to get rid of data that has missing attributes
    // mapValues will access only the values of our map
    // to access the items themselves
        // x._1, x._2, etc.
val processed_keyval = keyval.mapValues(x => 
    ((if(x._1 == "?") {"unknown"} else {x._1}), 
    (if(x._2 == "?") {-1} else {x._2.toDouble})))
    // in this case, x are our keys and values are y
val diesel_vehicles = processed_keyval.filter{case (x,y) => x == "diesel"}
    // we are comparing the second values of the value tuple
    // this comparison happens two lines at a time so,
    // x_1 and x_2 are the first line and y are the second line
val max_price_diesel = diesel_vehicles.reduceByKey{case (x,y) => 
    if(x._2 > y._2) {x} else {y}}
max_price_diesel.collect.foreach(println)