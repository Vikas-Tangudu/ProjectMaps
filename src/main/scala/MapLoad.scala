import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.apache.log4j._
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util._
import scala.collection.mutable.ListBuffer
import java.io.File
import org.apache.commons.lang.StringUtils

object MapLoad {
  val filenames: ListBuffer[String] = ListBuffer[String]()
  def getfilenames(files: Array[File]): Unit = {
    displayFiles1(files, "")
    for (i <- 0 until filenames.size) {
      filenames += StringUtils.chop(filenames(i))
    }
    filenames.remove(0, filenames.size / 2)
    for (i <- 0 until filenames.size) {
      filenames += "src/main/resources/" + filenames(i)
    }
    filenames.remove(0, filenames.size / 2)
  }
  def displayFiles1(files: Array[File], str: String): String = {
    for (filename <- files) {
      if (filename.isDirectory) { //filenames.add(filenameName());
        var str1: String = str + filename.getName + "/"
        //println(str1)
        filenames += str1
        //filenames.add(str)
        //println("Directory: " + filenames(0));
        str1 = displayFiles1(filename.listFiles, str1)
        str1 = ""
      }
      else {
        return ""
      }
    }
    ""
  }
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Getting all the File Paths--------------
    val files: Array[File] = new File("src/main/resources/").listFiles
    getfilenames(files)

    // Starting Spark Session---------------------
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkAndHive")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse2")
      .enableHiveSupport()
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark.sqlContext)

    //Creating Roads & Raliways DataFrame---------------
    def getmydf(inputpath: String): DataFrame = {
      val spatialRDD = ShapefileReader.readToGeometryRDD(spark.sparkContext, inputpath)
      val rawSpatialDf = Adapter.toDf(spatialRDD, spark)
      rawSpatialDf
    }
    val roadlist = ListBuffer[DataFrame]()
    val railwayslist = ListBuffer[DataFrame]()
    for (i <- 0 until filenames.size) {
      if (filenames(i).contains("_rds"))  roadlist += getmydf(filenames(i))
      if (filenames(i).contains("_rrd")) railwayslist += getmydf(filenames(i))
    }
    val finalroaddf = roadlist.reduce(_ union _)
    val finalrailwaysdf = railwayslist.reduce(_ union _)

    //  DB Name-------------------
    val date: Date = new Date
    val datearray: Array[String] = date.toString.replace(":", "_:").replace(" ", "_:").split(":")
    val dbname: String = "Maps_" + datearray(2) + datearray(1).toLowerCase + datearray(7) + "_"+ datearray(3) + datearray(4) + StringUtils.chop(datearray(5))
      println("DB Name : "+dbname)

    // SQL Properties:--------------
    val p = new Properties()
    p.put("user", "root")
    p.put("password", "root")
    p.put("url", "jdbc:mysql://localhost")
    p.put("driver", "com.mysql.cj.jdbc.Driver")

    // SQL Loading Area:--------------
    val dbcreate = s"CREATE DATABASE IF NOT EXISTS $dbname"
    // Connecting to MySql Server and creating database and empty tables--------------------
      Class.forName(p.getProperty("driver"))
      val conn: Connection = DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"))
      val stmt: PreparedStatement = conn.prepareStatement(dbcreate)
      try {stmt.execute}
      catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (conn != null) conn.close()
        if (stmt != null) stmt.close()
      }

    // Dumping the data from DF to MySql Server
    finalroaddf.write.mode(SaveMode.Overwrite).jdbc(p.getProperty("url"),s"$dbname.roads",p)
    finalrailwaysdf.write.mode(SaveMode.Overwrite).jdbc(p.getProperty("url"),s"$dbname.railways",p)

    // Getting data from MySql Database
    val roads_dataframe_mysql = spark.sqlContext.read.format("jdbc").option("url",p.getProperty("url")+ "/" + dbname)
                                .option("driver", p.getProperty("driver")).option("dbtable", "roads")
                                .option("user", p.getProperty("user")).option("password", p.getProperty("password")).load()
    roads_dataframe_mysql.createOrReplaceTempView("roads_dataframe_mysql")
    println("Roads Data::")
    spark.sql(s"select iso, count(*) as count from roads_dataframe_mysql group by iso").show
    val railways_dataframe_mysql = spark.sqlContext.read.format("jdbc").option("url",p.getProperty("url")+ "/" + dbname)
      .option("driver", p.getProperty("driver")).option("dbtable", "railways")
      .option("user", p.getProperty("user")).option("password", p.getProperty("password")).load()
    railways_dataframe_mysql.createOrReplaceTempView("railways_dataframe_mysql")
    println("Railways Data::")
    spark.sql("select iso, count(*) as count from railways_dataframe_mysql group by iso").show
  }
}