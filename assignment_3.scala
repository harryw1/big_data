val file = sc.textFile("/home/harry/spark_input/assignment_3.txt")
val split = file.map(x => (x.split(",")))
val average_map = split.map(x => (x(2).toInt, x(3).toInt))
val average = average_map.reduceByKey{case (a,b) => (a+b)/2}
val max_weight_map = split.map(x => (x(1) + "," + x(2), x(3).toInt))
val max = max_weight_map.values.max
val max_weight = max_weight_map.filter{case (a,b) => b == max}

println("Average Weight:")
average.collect.foreach(println)
println("Max Weight:")
max_weight.collect.foreach(println)
