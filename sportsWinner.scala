package SQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object sportsWinner {

  //Create a case class globally to be used inside the main method
  //Inferring the Schema Using Reflection.Automatically converting an RDD containing case classes to a DataFrame.
  // The case class defines the schema of the table. The names of the arguments to the case class are read using reflection
  // and become the names of the columns.
  case class Sports(firstname: String, lastname: String, sports: String, medal_type: String, age: Int, year: Long, countryid: String)

  // Main method - The execution entry point for the program
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Assignment_21_Sports")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Set the log level as warning
    spark.sparkContext.setLogLevel("WARN")

    //    println("Spark Session Object created")

    // For implicit conversions like converting RDDs and sequences  to DataFrames
    import spark.implicits._

    // Reading a file for the Holidays schema created above
    val data = spark.sparkContext
      .textFile("C:\\Users\\ssahay\\Documents\\Acadgild\\Completed_Assignments\\Day_21\\Sports_data.txt")

//    Storing the first line that is header of the file into another variable
    val header = data.first()

    // Selecting all other lines into data1 excluding the header.
    val data1 = data.filter(row => row != header)

    //Converting data1 into dataFrame splitting on comma character (,)
    // trim is used to eliminate leading.trailing whitespace & converting values into respective data types
     val sportsData =  data1.map(_.split(","))
      .map(x => Sports(x(0),x(1),x(2),x(3),x(4).trim.toInt,x(5).trim.toLong,x(6)))
      .toDF()

    //sportsData.show()

    //Converting the above created schema into an SQL view named sport
    sportsData.createOrReplaceTempView("sport")

//    1. What are the total number of gold medal winners every year?
    // Selecting year & couting the occurance of each year by filtering medal_type condition as gold.
    // grouping by year & ordering the result based upon the year.
    val a1 = spark.sql("Select year, count(year) as Gold_Medal_count from sport where medal_type = 'gold' group by year order by year")

    //Displaying the result set onto console
    a1.show


//    2. How many silver medals have been won by USA in each sport?
    // Selecting sports & count of sports as Silver_US from sports view. Provided the filter as medal_type = 'silver' &
    // coutry as USA, grouping by count & ordering the result based upon count of medals won.
    val a2 = spark.sql("select sports, count(sports) as Silver_US from sport where (medal_type = 'silver' AND countryid = 'USA') group by sports order by Silver_US")

    //Displaying the result set onto console
    a2.show


/*Using udfs on dataframe
1. Change firstname, lastname columns into
Mr.first_two_letters_of_firstname<space>lastname
for example - michael, phelps becomes Mr.mi phelps*/


//    sportsData.show

    /*
    New UDF with name "udfChangeColumns" is created using def udfChangeColumns = udf(),
    which accepts two parameters i.e. (firstname:String,lastname:String)
    */
    def udfChangeColumns = udf((firstname:String,lastname:String) => {
      /*substring(0,2) fetches only first two characters from entire firstname field
      and new variable twoCharsFromFirstName stores these two characters
      */
      val twoCharsFromFirstName = firstname.substring(0,2)

      /*
      "Mr."+twoCharsFromFirstName+" "+lastname -->> concatenates three strings i.e. "Mr.",twoCharsFromFirstName and lastname
       and concatenated string is stored in new variable "name"
      */
      val name = "Mr."+twoCharsFromFirstName+" "+lastname
      //value of "name" is returned from udf "udfChangeColumns"
      name
    })


    //sportsData.select(udfChangeColumns($"firstname",$"lastname").as("Name")).show

    /*withColumn appends a new column in newly created dataframe i.e. df
     here call to udf is made by passing two parameters i.e. firstname and lastname
     and new name i.e. "name" is given to second parameter which is returned by udf
    */
    val df= sportsData.withColumn("name",udfChangeColumns($"firstname",$"lastname"))

    //Two columns (firstname & lastname) of df are dropped and new df1 contains only undropped fields and corresponding data
    val df1 = df.drop("firstname","lastname")

    //To interchange the position of columns, select api is used with df
    val sol_df = df.select("name","sports","medal_type","age","year","countryid")

    //Displaying the result set onto console
    sol_df.show


    /*
    New UDF with name "udfAddColumns" is created using def udfAddColumns = udf(),
    which accepts two parameters i.e. (medal_type:String,age:Int)
    */

    def udfAddColumns = udf((medal_type:String,age:Int) => {

      // Checking if medal_type is gold & age is greater than or equal 32 return value as "pro"
      if (medal_type.equals("gold") && age >= 32 ) "pro"

      // Checking if medal_type is gold & age is lesser than or equal 31 return value as "amateur"
      else if(medal_type.equals("gold") && age <= 31 ) "amateur"

      // Checking if medal_type is silver & age is greater than or equal 32 return value as "expert"
      else if(medal_type.equals("silver") && age >= 32 ) "expert"

      // Checking if medal_type is silver & age is lesser than or equal 31 return value as "rookie"
      else if(medal_type.equals("silver") && age <= 31 ) "rookie"

      // If none of above conditions are met just return "NA'
      else "NA"
    })

    /*withColumn appends a new column in newly created dataframe i.e. added_df
    here call to udf is made by passing two parameters i.e. medal_type and age
    and new name i.e. "Ranking" is given to second parameter which is returned by udf
   */
    val added_df = sportsData.withColumn("Ranking",udfAddColumns($"medal_type",$"age"))

    //Displaying the result set onto console
    added_df.show
  }
}
