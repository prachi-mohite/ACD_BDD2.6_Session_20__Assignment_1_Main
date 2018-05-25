import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{lit, struct}

object Assignment_20_SQL {
  //We have to read the dataset values provided in text files and create dataframes from it
  //for the same we have to create the case classes where attribute names will provide
  // column names and values will get mapped as per the datatype

  //Case Class for Holidays
  case class Holidays (UserID:Int,Country_Name_Dept:String,Country_Name_Arrival:String,modeOfTravel:String,Distance:Int, Year:Int)

  //Case Class for Transport Details
  case class Transport_Details(Transport_Mode:String,Transport_Exp:Int)

  //Case Class for User Details
  case class User_Details(UserID:Int,User_Name:String,Age:Int)

  def main(args:Array[String]): Unit = {


    //Let us create a spark session object
    //Create a case class globally to be used inside the main method
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL Assignment 20")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

       println("spark session object is created")

    import spark.implicits._
    //Read the Holiday Details from Local file

    val data = spark.sparkContext.textFile("E:\\Prachi IMP\\Hadoop\\Day 18 Spark\\Day - 20 Spark SQL\\S20_Holidays.txt")
    //Create Holdiays DF
    //Here we have mentioned name of Attributes to be mapped , however it is not mandatory to do So.
    //If Attribute names are not mentioned then follow sequence of attributes
    //toDF() without any column names indicates , need to take all columns from dataset.
    val holidaysDF = data.map(_.split(",")).map(x=>Holidays(UserID = x(0).toInt,Country_Name_Dept = x(1),Country_Name_Arrival = x(2),
                      modeOfTravel = x(3),Distance = x(4).toInt,Year = x(5).toInt)).toDF()

    //Printing data of Holidays DF
    holidaysDF.show()

    //Create Transport Details DF by loading the Transport_Details file
    val transportDetailsDF = spark.sparkContext.textFile("E:\\Prachi IMP\\Hadoop\\Day 18 Spark\\Day - 20 Spark SQL\\S20_Transport.txt").
      map(_.split(",")).map(x=>Transport_Details(Transport_Mode = x(0),Transport_Exp = x(1).toInt)).toDF()

    //Printing data of Transport Mode DF
    transportDetailsDF.show()

    //Create USer Details DF by loading the User file
    val userDetailsDF = spark.sparkContext.textFile("E:\\Prachi IMP\\Hadoop\\Day 18 Spark\\Day - 20 Spark SQL\\S20_User_Details.txt").
      map(_.split(",")).map(x=>User_Details(UserID = x(0).toInt,User_Name = x(1),Age = x(2).toInt)).toDF()

    //Printing data of Transport Mode DF
    userDetailsDF.show()

    //Task 1.1 : What is the distribution of the total number of air-travelers per year
    //We have get the count of Users travelled vai air per year
    //Need to query on Holidays DF with grouped on Year and transport mode should be airplane

    //This is by using filter and group by operations on DataFrame
    holidaysDF.filter("modeOfTravel='airplane'").groupBy("Year").count().show()

    //Below approach is by using SQL in spark
    holidaysDF.createOrReplaceTempView("Holiday_Data")
    println("Using SQL & Temp View")
    spark.sql("Select year, count(Year) a from Holiday_Data where modeOfTravel='airplane' group By Year ").show()

    //Task 1.2 : What is the total air distance covered by each user per year
    //We have to get total distance per user
    //Group holidays data on user id and sum the distance
    //to get the user names we have to join the two tables User_Details & Holidays

    //creating Or replacing the view
    userDetailsDF.createOrReplaceTempView("Users_Data")

    //Approach 2: Below is done by using SQL
    spark.sql("Select User_Name, Year, sum(Distance) a from Holiday_Data HD JOIN Users_Data UD ON " +
             "HD.UserID==UD.UserID  group By HD.UserID, HD.Year, UD.User_Name").show()

    //Approach 1: By joinging two DFs
    println("Below Result is after joining two Data frames")
    holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
        .groupBy("HD.UserID","HD.Year","UD.User_Name").sum("Distance").show()



    //Task 1.3 : Which user has travelled the largest distance till date
    //Approach 1: Using Spark SQL Operations
      val result3 = holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID").
      groupBy("HD.UserID","HD.Year","UD.User_Name").sum("Distance")
      .withColumnRenamed("sum(Distance)","MaxDistance")
      .sort(desc("MaxDistance")).take(1).mkString(",")
       println(result3)

    //Approach 2: Using SQL Statements
    val result4 = spark.sql("Select User_Name, Year, sum(Distance) as MaxDistance from Holiday_Data HD JOIN Users_Data UD ON " +
    "HD.UserID==UD.UserID  group By HD.UserID, HD.Year, UD.User_Name order by MaxDistance desc").take(1).mkString(",")
    println(result4)

      ////Task 1.4: What  is the most preferred destination for all users.
      //Group the holiday list on basis of destination and get the count

      //Approach 1: Using Spark SQL Operations
      holidaysDF.groupBy("Country_Name_Arrival").count()
        .sort(desc("count")).show(1)

      //Approach 2: Using SQL
      spark.sql("Select  Country_Name_Arrival,count(Country_Name_Arrival) as Fav_Destination from Holiday_Data " +
        "group by Country_Name_Arrival order by Fav_Destination desc").show(1)




      //Task 1.5 : Which route is generating the most revenue per year
      //Need to join DF for Transport Mode and Holidays on Transport mode as key and group on transport mode
      //get the sum of fair for that transport

      //Approach 1: Using Spark SQL Operations
      //First create a new DF where two columns Dept Country and Arrival country should be kep in one column to get distinct routes
      val routesDF= holidaysDF.withColumn("Route",struct("Country_Name_Dept","Country_Name_Arrival")).toDF()
      routesDF.as('HD).join(transportDetailsDF.as('TD),$"TD.Transport_Mode"===$"HD.modeOfTravel")
            .groupBy("HD.Route").sum("Transport_Exp").
            withColumnRenamed("sum(Transport_Exp)","Total_Exp").sort(desc("Total_Exp")).show(1)

      //Approach 2: with SQL
      routesDF.createOrReplaceTempView("Routes_Data")
      transportDetailsDF.createOrReplaceTempView("Transport_Data")
      spark.sql("Select Route,sum(Transport_Exp) from Routes_Data RD JOIN Transport_Data TD ON RD.modeOfTravel = TD.Transport_Mode " +
                        "group by RD.Route").show(1)



     //Task 1.6 What is the total amount spent by every user on air-travel per year
     //Need to User Details  , Transport Details and holidays details
     //group on user per year to get sum of expenses on air travel

     //Approach 1: Using spark SQL operations
     holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
                 .join(transportDetailsDF.as("TD"),$"HD.modeOfTravel"===$"TD.Transport_Mode")
                 .groupBy("UD.UserID","UD.User_Name","HD.Year").sum("Transport_Exp")
                 .sort("UserID","Year")show()



 //Task 1.7 Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most
 //every year.
 //We need to join holidays and user details and get three result set for different age groups
 // join those and get the final result based on travelling count is more

 //Get result (DF) for group less 20
 val grpBelow20 = holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
                 .filter("UD.Age<20").withColumn("AgeGroup",lit("Less20")).toDF()
 grpBelow20.show()

 //Get result (DF) for group between 20 and 35
 val grpBet20And35 = holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
   .filter("UD.Age between 20 And 35").withColumn("AgeGroup",lit("Between20And35")).toDF()
 grpBet20And35.show()

 //Get result (DF) for group greater than 35
 val grpAbove35 = holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
   .filter("UD.Age>35").withColumn("AgeGroup",lit("above35")).toDF()
 grpAbove35.show()

 grpBelow20.union(grpBet20And35).union(grpAbove35).groupBy("AgeGroup").count().sort(desc("count")).
   sort(desc("count")).show(1)

 //another Approach of Case Statement
 holidaysDF.as('HD).join(userDetailsDF.as('UD),$"UD.UserID"===$"HD.UserID")
           .select(when($"Age"<20,"LessThan20").
             when($"Age" > 20 && $"Age"<35,"Between20And35").
             when($"Age">35,"Above35").alias("AgeGroup")).
           groupBy("AgeGroup").count().sort(desc("count")).show(1)


  }

}
