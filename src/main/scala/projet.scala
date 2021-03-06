import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc}


object project extends App {

  val ss = SparkSession
    .builder()
    .appName("ProjetSpark")
    .master("local[*]")
    .getOrCreate()

  val data = ss.read
    .option("header","true")
    .option("inferSchema","true")
    .csv("MARITAL_STATUS_2019.csv")

  data.printSchema()
  println("Taille du DataSet :" + data.count())

  val All = data.select("Country","Sex","MaritalStatus","AgeGroup")
  All.printSchema()
  All.show(10)

  val Married_by_country = All
                            .filter(All("MaritalStatus") like "Married")
                            .groupBy("Country")
                            .count()
                            .withColumnRenamed("count", "Married")
                            .withColumnRenamed("Country", "Country1")

  val Divorced_by_country = All.
                            filter(All("MaritalStatus") like "Divorced")
                            .groupBy("Country")
                            .count()
                            .withColumnRenamed("count", "Divorced")
                            .withColumnRenamed("Country", "Country2")

  val Single_by_country = All
                            .filter(All("MaritalStatus") like "Single")
                            .groupBy("Country")
                            .count()
                            .withColumnRenamed("count", "Single")
                            .withColumnRenamed("Country", "Country3")

  val population_by_country = All
                            .groupBy("Country")
                            .count()
                            .withColumnRenamed("count", "Population")
                            .withColumnRenamed("Country", "Country4")

  val statistics_by_countries_part_one = Married_by_country
                              .join(Divorced_by_country, Married_by_country("Country1") === Divorced_by_country("Country2"), "cross")

  val statistics_by_countries_part_two = statistics_by_countries_part_one
                            .join(Single_by_country, statistics_by_countries_part_one("Country1") === Single_by_country("Country3"), "cross")

  val statistics_by_countries_part_three = statistics_by_countries_part_two
                            .join(population_by_country, statistics_by_countries_part_two("Country1") === population_by_country("Country4"), "cross")

  //All Statistics
  val statistics_by_countries = statistics_by_countries_part_three
                              .select(col("Country1").as("Country")
                                , col("Population")
                                , col("Married")
                                , col("Divorced")
                                , col("Single"))

  statistics_by_countries.printSchema()
  statistics_by_countries.show(10)

  // Ten countries wich have a maximum number of married
   val ten_countries_max_married = Married_by_country
                                 .withColumnRenamed("Country1", "Countries")
                                 .sort(desc("count"))
                                 .limit(10)
  ten_countries_max_married.show()

  //Ten countries wich have a maximum number of divorced
  val ten_countries_max_divorced = Divorced_by_country
                                .withColumnRenamed("Country2", "Countries")
                                .sort(desc("count"))
                                .limit(10)
  ten_countries_max_divorced.show()

  //Ten countries wich have a maximum number of single
  val ten_countries_max_single= Single_by_country
                            .withColumnRenamed("Country3", "Countries")
                            .sort(desc("count"))
                            .limit(10)
  ten_countries_max_single.show()
//
//
////   statistics.repartition(1).write.csv("C://Users//asmab//OneDrive//Bureau//ESGI//spark//proje//statistics//statistics.csv")
statistics_by_countries.repartition(1)
    .write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
  .save("C://Users//asmab//OneDrive//Bureau//ESGI//spark//projet//statistics//statistics.csv")
//
  //Analyse with age
  val Married_by_age = All
                      .filter(All("MaritalStatus") like "Married")
                      .groupBy("AgeGroup")
                      .count()
                      .withColumnRenamed("count","NumberOfMarried")
                      .withColumnRenamed("AgeGroup","AgeGroup1")

  val Divorced_by_age = All
                      .filter(All("MaritalStatus") like "Divorced")
                      .groupBy("AgeGroup").count()
                      .withColumnRenamed("count","NumberOfDivorced")
                      .withColumnRenamed("AgeGroup","AgeGroup2")

  val Single_by_age = All
                      .filter(All("MaritalStatus") like "Single")
                      .groupBy("AgeGroup")
                      .count()
                      .withColumnRenamed("count","NumberOfSingle")
                      .withColumnRenamed("AgeGroup","AgeGroup3")

  val max_married_by_age = Married_by_age
                      .sort(desc("NumberOfMarried"))
                      .limit(5)
  max_married_by_age.show()

  val max_Divorced_by_age = Divorced_by_age
                      .sort(desc("NumberOfDivorced"))
                      .limit(5)
  max_Divorced_by_age.show()

  val max_Single_by_age = Single_by_age
                    .sort(desc("NumberOfSingle"))
                    .limit(5)
  max_Single_by_age.show()

  val population_by_age = All
    .groupBy("AgeGroup")
    .count()
    .withColumnRenamed("count", "Population")
    .withColumnRenamed("AgeGroup", "AgeGroup4")


  val statistics_by_age_part_one = Married_by_age
    .join(Divorced_by_age, Married_by_age("AgeGroup1") === Divorced_by_age("AgeGroup2"), "cross")

  val statistics_by_age_part_two = statistics_by_age_part_one
    .join(Single_by_age, statistics_by_age_part_one("AgeGroup1") === Single_by_age("AgeGroup3"), "cross")

  val statistics_by_age_part_three = statistics_by_age_part_two
    .join(population_by_age, statistics_by_age_part_two("AgeGroup1") === population_by_age("AgeGroup4"), "cross")

  //All Statistics
  val statistics_by_age = statistics_by_age_part_three
    .select(col("AgeGroup1").as("AgeGroup")
      , col("Population")
      , col("NumberOfMarried")
      , col("NumberOfDivorced")
      , col("NumberOfSingle")
    )

  statistics_by_age.show(10)

  statistics_by_age.repartition(1)
    .write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .save("C://Users//asmab//OneDrive//Bureau//ESGI//spark//projet//statistics//statistics_by_age.csv")

  System.in.read
  ss.stop()


}