import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, count, desc, expr, max}


object project extends App {
  val ss = SparkSession
    .builder()
    .appName("ProjetSpark")
    .master("local[*]")
    .getOrCreate()

  val All = ss.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("C://Users//asmab//OneDrive//Bureau//ESGI//spark//projet//data_etat_civil_2019.csv")

  //    DataFrame.printSchema
  //      DataFrame.show(20)

  val Married_by_country = All.filter(All("MaritalStatus") like "Married").groupBy("Country").count().withColumnRenamed("count", "Married").withColumnRenamed("Country", "Country1")
  val Divorced_by_country = All.filter(All("MaritalStatus") like "Divorced").groupBy("Country").count().withColumnRenamed("count", "Divorced").withColumnRenamed("Country", "Country2")
  val Single_by_country = All.filter(All("MaritalStatus") like "Single").groupBy("Country").count().withColumnRenamed("count", "Single").withColumnRenamed("Country", "Country3")
  val population_by_country = All.groupBy("Country").count().withColumnRenamed("count", "Population").withColumnRenamed("Country", "Country4")

  val statistics_part_one = Married_by_country.join(Divorced_by_country, Married_by_country("Country1") === Divorced_by_country("Country2"), "cross")
  val statistics_part_two = statistics_part_one.join(Single_by_country, statistics_part_one("Country1") === Single_by_country("Country3"), "cross")
  val statistics_part_three = statistics_part_two.join(population_by_country, statistics_part_two("Country1") === population_by_country("Country4"), "cross")

  val statistics = statistics_part_three.select(col("Country1").as("Country"), col("Population"), col("Married"), col("Divorced"), col("Single"))

  statistics.show(20)
  //Married_by_country.sort(desc("count")).show(10)
  //  df.coalesce(1).write.csv("C://Users//asmab//OneDrive//Bureau//ESGI//spark//projet//statistics//statistics.csv")
  statistics.write
    .option("header", "true")
    .csv("file:///Users//asmab//OneDrive//Bureau//ESGI//spark//projet//statistics//statistics.csv")
}