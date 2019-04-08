// Input file
val file = sc.textFile("/home/arafuls/Desktop/Scala/Projects/20190401/input.txt");
val keyval = file.map(x => x.split(","));        // Create (key, value) pairs
val result = keyval.map(x => (x(0), x(1).toInt));     // Convert value from string to int

// val keysum = result.reduceByKey{ case (a,b) => (a+b) };
// val keymax = result.reduceByKey{ case (a,b) => if(a>b){a} else{b} };
val newresult = result.mapValues(x => (x,1));

// To filter out the max value
// val max = result.values.max;
// val newresult = result.filter{ case (a,b) => b==max };

// (A, (20, 1))
// (A, (30, 1))
// x._1 is 20
// y._1 is 30
// val NewResult = newresult.reduceByKey{ case (x,y) => ((x._1 + y._1),0) };

// Reduce tuple
val tmp = newresult.reduceByKey{ case (x,y) => ((x._1 + y._1),0) };
val NewResult = tmp.mapValues( x => x._1 );

// Collect and print
// result.collect.foreach(println);    // Lists everything
// keysum.collect.foreach(println);    // Lists reduced sum for each key
// keymax.collect.foreach(println);    // Lists reduced max for each key
// newresult.collect.foreach(println);    // Lists everything but in format (A, (20, 1))
// newresult.collect.foreach(println);    // Lists a single max value using filtering
// NewResult.collect.foreach(println);    // Lists reduced sum for each key in format (A, (20, 1)) into (A, (60, 0))
NewResult.collect.foreach(println);    // Reduce the tuple from (A, (20, 1)) to (A, 20)

// Notes
// When reducing, within the case (a,b) we are comparing a value (a)
// against another value (b) where the key is the same
