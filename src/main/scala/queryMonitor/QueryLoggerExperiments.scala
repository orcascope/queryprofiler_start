package queryMonitor

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}


object QueryLoggerExperiments extends App{

  //  Get spark session
  val spark = SparkSession.builder()
    .master("local")
    .appName("testQueryListener")
    .getOrCreate()

  //  Get regularixer object
  val myreg = new Regularizer()

  //  Code to generate a sample dataframe and a temp view
  var stu_rdd1 =spark.sparkContext.parallelize(Seq(Row(101, "alex",88.56),Row(101, "alex",68.32),Row(102, "john",68.32)))
  var schema_list=List(("id","int"),("name","string"),("marks","double"))
  var schema=new StructType()
  schema_list.map(x=> schema=schema.add(x._1,x._2))
  val student = spark.createDataFrame(stu_rdd1,schema)
  student.createOrReplaceTempView("my_table")

  // Run a SQL and get the logical plan
  val stu_2 = spark.sql("select id, highest_mark as top_mark from (select id, max(marks) as highest_mark from my_table group by id) as t")
  val plan2 = stu_2.queryExecution.optimizedPlan

  println("~~~~~~~~~~~~Plan before regularizing~~~~~~~~~~~~~~~~~~")
  println(plan2.treeString)

  val reg_plan = myreg.execute(plan2)
  println("~~~~~~~~~~~~Plan after regularizing~~~~~~~~~~~~~~~~~~")
  println(reg_plan.treeString)
}
