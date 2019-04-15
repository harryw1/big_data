// 10, 14, 15
// a) The number of male who earns more than 50k in the US
// b) The number of Female who earns more than 50k in the US
// c) The number of male who earns more than 50k in Canada
// d) The number of Female who earns more than 50k in the Canada

val input_file = sc.textFile("/home/harry/Documents/scala_files/input/exam_2_part_2.txt")
val split = input_file.map(x => x.split(", "))
    // gender is key and country, income and count are values
val mapped_file = split.map(x => 
    (x(9), 
    (x(13), x(14), 1)))
    // a are the keys, b are the values
val males = mapped_file.filter{case (a,b) => 
    a == "Male"}
val females = mapped_file.filter{case (a,b) =>
    a == "Female"}

    // filtering the two countries out
val american_male = males.filter{case (a,b) =>
    b._1 == "United-States"}
val american_female = females.filter{case (a,b) =>
    b._1 == "United-States"}
val canadian_males = males.filter{case (a,b) =>
    b._1 == "Canada"}
val canadian_females = females.filter{case (a,b) =>
    b._1 == "Canada"}

val high_income_male_us = american_male.filter{case (a,b) => 
    b._2 == ">50K"}
val high_income_female_us = american_female.filter{case (a,b) => 
    b._2 == ">50K"}
val high_income_female_canada = canadian_females.filter{case (a,b) => 
    b._2 == ">50K"}
val high_income_male_canada = canadian_males.filter{case (a,b) => 
    b._2 == ">50K"}

val count_1 = high_income_male_us.reduceByKey{case (a,b) => 
    (a._1, a._2, a._3 + b._3)}
val count_2 = high_income_male_canada.reduceByKey{case (a,b) => 
    (a._1, a._2, a._3 + b._3)}
val count_3 = high_income_female_us.reduceByKey{case (a,b) => 
    (a._1, a._2, a._3 + b._3)}
val count_4 = high_income_female_canada.reduceByKey{case (a,b) => 
    (a._1, a._2, a._3 + b._3)}

count_1.collect.foreach(println)
count_2.collect.foreach(println)
count_3.collect.foreach(println)
count_4.collect.foreach(println)