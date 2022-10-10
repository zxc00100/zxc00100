# Spark SQL

## 一、概述

<http://spark.apache.org/docs/latest/sql-programming-guide.html>

Spark SQL是Spark中一个模块，用以对结构化数据进行处理。SparkSQL在RDD之上抽象出来Dataset/Dataframe 这两个类提供了类似RDD的功能，也就意味用户可以使用map、flatMap、filter等高阶算子，同时也通过了基于列的命名查询，也就是说Dataset/DataFrame提供了两套操作数据的API，这些API可以给Saprk引擎要提供更多信息，系统可以根据这些信息对计算实现一定的优化。目前Spark SQL提供了两种交互方式：1） SQL 脚本 ，2） Dataset API (strong-typed类型、untyped类型操作)



### Datasets & DataFrames 

**Dataset**是一个分布式数据集，Dataset是在spark-1.6提出新的API，该API构建在RDD（strong type，使用lambda表达式）之上同时可以借助于Spark SQL对执行引擎的优点，使得使用Dateset执行一些数据的转换比直接使用RDD算子功能和性能都有所提升。因此我们可以认为==Dateset就是一个加强版本的RDD。Dataset除了可以使用JVM中数组|集合对象创建之外，也可以将任意的一个RDD转换为Dataset。==Python does not have the support for the Dataset API==. 



DataFrames 是Dataset的一种特殊情况。比如 Dataset中可以存储任意对象类型的数据作为Dataset的元素。但是Dataframe的元素只有一种类型Row类型，这种基于Row查询和传统数据库中ResultSet操作极其相似。因为Row类型的数据表示Dataframe的一个元素，类似数据库中的一行，这些行中的元素可以通过下标或者column name访问。由于Dateset是API的兼容或者支持度上不是多么的好，但是Dataframe在API层面支持的Scala、Java、R、Python支持比较全面。



## 二、入门案例

### 导入开发依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.11</artifactId>
    <version>2.4.4</version>
</dependency>
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.11</artifactId>
    <version>2.4.4</version>
</dependency>
```

### 开发应用

```scala
package com.baizhi.sql

import org.apache.spark.sql.SparkSession

/**
  * 第一个spark sql的单词计数案例
  *
  */
object SparkSQLWordCount {
  def main(args: Array[String]): Unit = {
    // 1. sparkSession是spark sql应用入口，内部封装了sparkconf和sparkContext
    val spark = SparkSession
      .builder()
      .appName("the first spark sql example")
      .master("local[*]")
      .getOrCreate()

    // 2. 创建Dataset
    val rdd = spark.sparkContext.makeRDD(List("Hello Hadoop", "Hello Scala")).flatMap(_.split(" ")).map((_, 1))
    // 3. rdd ==> Dataset
    // 4. 导入隐式转换
    import spark.implicits._
    val dataset = rdd.toDS // (Hello 1) | (Hadoop 1)

    // 5. 对dataset进行sql处理
    dataset
      .where("_1 !='Scala'")
      .groupBy("_1")
      .sum("_2")
      .withColumnRenamed("_1", "word")
      .withColumnRenamed("sum(_2)", "num")
      .show()
    /*
    +------+-------+
    |    _1|sum(_2)|
      +------+-------+
    | Hello|      2|
    | Scala|      1|
    |Hadoop|      1|
    +------+-------+
    */

    //------------------------------------------------------------------------------
    val dataFrame = dataset.toDF()
    dataFrame.createOrReplaceTempView("t_word")
    dataFrame.sqlContext.sql("select _1 as word, count(_2) as num from t_word group by _1").show()
    /*
    +------+---+
    |  word|num|
    +------+---+
    | Hello|  2|
    | Scala|  1|
    |Hadoop|  1|
    +------+---+
     */
    // 6. 关闭spark sql应用
    spark.stop()
  }
}
```



## 三、创建数据源

### 创建Dataset

Dataset类似于RDD，不同的是Spark SQL有一套自己的序列化规范独立于Spark RDD（Java/Kryo序列化）之上称为==Encoders(编码器)==。不同于SparkRDD序列化，由于Dataset支持无类型操作，用户无需获取操作的类型，操作仅仅是列名，因为Spark SQL在执行算子操作的时候可以省略反序列化的步骤，继而提升程序执行效率。

#### 样例类创建

```scala
// 通过样例类创建dataset
val userList = List(User(1, "zs", true), User(2, "ls", false), User(3, "ww", true))
import spark.implicits._
val dataset = userList.toDS // 通过样例类构建的dataset字段名是样例类属性名
dataset.show()

//-----------------------------------------------
+---+----+-----+
| id|name|  sex|
+---+----+-----+
|  1|  zs| true|
|  2|  ls|false|
|  3|  ww| true|
+---+----+-----+
```

#### Tuple创建

```scala
// 通过元组创建dataset
val tupleList = List((1, "zs", true), (2, "ls", false), (3, "ww", true))
import spark.implicits._
val dataset = tupleList.toDS
dataset.show()
//-----------------------------------------------
+---+---+-----+
| _1| _2|   _3|
+---+---+-----+
|  1| zs| true|
|  2| ls|false|
|  3| ww| true|
+---+---+-----+
```

#### JSON创建

```scala
// 通过JSON创建
val dataset = spark.read.json("file:///G:\\IDEA_WorkSpace\\scala-workspace\\spark-day7\\src\\main\\resources").as("user")
//-----------------------------------------------
+---+----+-----+
| id|name|  sex|
+---+----+-----+
|  4| zs2| true|
|  5| ls2|false|
|  6| ww2| true|
|  1|  zs| true|
|  2|  ls|false|
|  3|  ww| true|
+---+----+-----+
```

#### RDD创建

```scala
// 通过RDD【元组集合】创建
val rdd = spark.sparkContext.makeRDD(List((1, "zs", true), (2, "ls", false), (3, "ww", true)))
import spark.implicits._
val dataset = rdd.toDS
dataset.show()

// 通过RDD【样例类集合】创建
val rdd = spark.sparkContext.makeRDD(List(User(1, "zs", true), User(2, "ls", false), User(3, "ww", true)))
import spark.implicits._
val dataset = rdd.toDS
dataset.show()
```



### 创建Dataframe

DataFrame是一个命名列的数据集，用户可以直接操作column 因此几乎所有Dataframe推荐操作都是 无类型操作 。用户也可以把一个Dataframe看做是 Dataset[Row] 类型的数据集。 

#### Json创建

```scala
// 通过JSON创建
val df = spark.read.json("file:///G:\\IDEA_WorkSpace\\scala-workspace\\spark-day7\\src\\main\\resources")
df.printSchema()  // df结构
df.show()
//-------------------------------------------------------------------
root
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- sex: boolean (nullable = true)

+---+----+-----+
| id|name|  sex|
+---+----+-----+
|  4| zs2| true|
|  5| ls2|false|
|  6| ww2| true|
|  1|  zs| true|
|  2|  ls|false|
|  3|  ww| true|
+---+----+-----+
```

#### 样例类创建

```scala
val userDF=List(User2(1,"zs",true)).toDF()
userDF.show()
//--------------------------------------------------------------------
+---+----+----+
| id|name| sex|
+---+----+----+
|  1|  zs|true|
+---+----+----+
```

#### Tuple创建

```scala
var userDF=List((1,"zs",true)).toDF("id","name","sex") 
userDF.show()
//--------------------------------------------------------------------
+---+----+----+
| id|name| sex| +---+----+----+
| 1| zs|true| +---+----+----+
```

#### RDD创建

```scala
// RDD【元组】转换创建
val rdd = spark.sparkContext.makeRDD(List((1,"zs"),(2,"ls")))
val df = rdd.toDF("id","name")
df.printSchema()
df.show()
//--------------------------------------------------------------------
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)

+---+----+
| id|name|
+---+----+
|  1|  zs|
|  2|  ls|
+---+----+

// RDD【样例类】转换创建
var userDF= spark.sparkContext.parallelize(List(User(1,"zs",true))).toDF("id","uname","sex") 
userDF.show()
//--------------------------------------------------------------------
+---+-----+----+
| id|uname| sex| 
+---+-----+----+
| 1 | zs  |true| 
+---+-----+----+

// 通过RDD[Row]创建DF
val rdd = spark.sparkContext.parallelize(List((1, "zs", true), (2, "ls", false))).map(t => Row(t._1, t._2, t._3))
val schema = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("sex", BooleanType)
val df = spark.createDataFrame(rdd, schema)
df.show()
spark.stop()
//--------------------------------------------------------------------
+---+----+-----+
| id|name|  sex|
+---+----+-----+
|  1|  zs| true|
|  2|  ls|false|
+---+----+-----+

// 通过JAVA Bean创建DF
val personList = List(new Person(1, "zs"), new Person(2, "ls"))
val rdd = spark.sparkContext.makeRDD(personList)
val df = spark.createDataFrame(rdd, classOf[Person])
df.show()
//--------------------------------------------------------------------
+---+----+
| id|name|
+---+----+
|  1|  zs|
|  2|  ls|
+---+----+

// 通过RDD[case class]创建DF
case class ScalaUser(id:Int,name:String,sex:Boolean)
var userRDD:RDD[ScalaUser]=spark.sparkContext.makeRDD(List(ScalaUser(1,"zs",true)))
var userDF=spark.createDataFrame(userRDD)
userDF.show()
//--------------------------------------------------------------------
+---+----+----+
| id|name| sex| 
+---+----+----+
| 1 | zs |true|
+---+----+----+

// 通过RDD[Tuple] 创建DF
var userRDD:RDD[(Int,String,Boolean)]=spark.sparkContext.makeRDD(List((1,"zs",true))) 
var userDF=spark.createDataFrame(userRDD) 
userDF.show()
//--------------------------------------------------------------------
+---+---+----+
| _1| _2| _3| 
+---+---+----+
| 1| zs|true| 
+---+---+----+

```



## 四、SQL操作

### DataFrame操作(untyped)

#### printSchema()

```scala
object DataframeOperationTest {
  def main(args: Array[String]): Unit = {
    val sparkSql = SparkSession.builder().appName("df operation").master("local[*]").getOrCreate()
    import sparkSql.implicits._

    val rdd = sparkSql.sparkContext.makeRDD(List((1,"zs",1000.0,true),(2,"ls",2000.0,false),(3,"ww",3000.0,false)))

    val df = rdd.toDF("id","name","salary","sex")

    // 打印df的结构
    df.printSchema()

    sparkSql.stop()
  }
}

//-----------------------------------------------------------------------------
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = true)
 |-- salary: double (nullable = false)
 |-- sex: boolean (nullable = false)
```

#### show()

```scala
// 默认输出df中前20行数据
df.show()
//-----------------------------------------------------------------------------
+---+----+------+-----+
| id|name|salary|  sex|
+---+----+------+-----+
|  1|  zs|1000.0| true|
|  2|  ls|2000.0|false|
|  3|  ww|3000.0|false|
+---+----+------+-----+
```

#### Select()

```scala
// 查询指定的字段
// df.select("id","name","sex").show()
// $是另外一种写法[隐式转换] 字符串列名==>Column对象
df.select($"id",$"name",$"sex").show()
//-----------------------------------------------------------------------------
+---+----+-----+
| id|name|  sex|
+---+----+-----+
|  1|  zs| true|
|  2|  ls|false|
|  3|  ww|false|
+---+----+-----+
```

#### SelectExpr()

```scala
// 查询指定字段【表达式】
// df.selectExpr("name as username").show()
// df.selectExpr("sum(salary)").show()
df.selectExpr("id","name as username","salary","salary*12").show()

//-----------------------------------------------------------------------------
+---+--------+------+-------------+
| id|username|salary|(salary * 12)|
+---+--------+------+-------------+
|  1|      zs|1000.0|      12000.0|
|  2|      ls|2000.0|      24000.0|
|  3|      ww|3000.0|      36000.0|
+---+--------+------+-------------+
```

#### withColumn()

```scala
// 添加或者替换【列名相同】字段
df.select($"id",$"name",$"salary")
    // .withColumn("year_salary",$"salary"*12) // 添加列
    .withColumn("salary",$"salary"*12) // 替换已存在的列
    .show()

//-----------------------------------------------------------------------------
+---+----+-------+
| id|name| salary|
+---+----+-------+
|  1|  zs|12000.0|
|  2|  ls|24000.0|
|  3|  ww|36000.0|
+---+----+-------+
```

#### withColumnRenamed()

```scala
df.select($"id", $"name", $"salary")
      // .withColumn("year_salary",$"salary"*12) // 添加列
      .withColumn("salary", $"salary" * 12) // 替换已存在的列
      .withColumnRenamed("name","username")
      .withColumnRenamed("id","user_id")
      .show()

//-----------------------------------------------------------------------------
+-------+--------+-------+
|user_id|username| salary|
+-------+--------+-------+
|      1|      zs|12000.0|
|      2|      ls|24000.0|
|      3|      ww|36000.0|
+-------+--------+-------+
```

#### Drop()

```scala
df.select($"id", $"name", $"salary")
      // .withColumn("year_salary",$"salary"*12) // 添加列
      .withColumn("salary", $"salary" * 12) // 替换已存在的列
      .withColumnRenamed("name", "username")
      .withColumnRenamed("id", "user_id")
      .drop($"username")
      .show()
//-----------------------------------------------------------------------------

+-------+-------+
|user_id| salary|
+-------+-------+
|      1|12000.0|
|      2|24000.0|
|      3|36000.0|
+-------+-------+
```

#### DropDuplicates()

~~~scala
// 删除重复数据 DropDuplicates  类似于数据库中distinct【重复数据只保留一个】
val df2 = sparkSql.sparkContext.makeRDD(List((1, "zs", 1000.0, true), (2, "ls", 2000.0, false), (3, "ww", 2000.0, false),(4, "zl", 2000.0, false))).toDF("id","name","salary","sex")
df2.select($"id", $"name", $"salary",$"sex")
// .withColumn("year_salary",$"salary"*12) // 添加列
    .withColumn("salary", $"salary" * 12) // 替换已存在的列
    .withColumnRenamed("name", "username")
    .withColumnRenamed("id", "user_id")
    .dropDuplicates("salary","sex")
    .show()

//-----------------------------------------------------------------------------
+-------+--------+-------+-----+
|user_id|username| salary|  sex|
+-------+--------+-------+-----+
|      2|      ls|24000.0|false|
|      1|      zs|12000.0| true|
+-------+--------+-------+-----+
~~~

#### OrderBy()| Sort()

```scala
// 排序OrderBy()| Sort()
df
    .select($"id", $"name", $"salary", $"sex")
    //.orderBy($"salary" desc)
    //.orderBy($"salary" asc)
    //.orderBy($"salary" asc,$"id" asc)
    .sort($"salary" desc)  // 等价于OrderBy
    .show()

//-----------------------------------------------------------------------------
+---+----+------+-----+
| id|name|salary|  sex|
+---+----+------+-----+
|  3|  ww|3000.0|false|
|  2|  ls|2000.0|false|
|  1|  zs|1000.0| true|
+---+----+------+-----+
```

#### GroupBy ()

```scala
// 分组groupBy()
df
    .groupBy($"sex")
    .sum("salary")
    .show()
//-----------------------------------------------------------------------------
+-----+-----------+
|  sex|sum(salary)|
+-----+-----------+
| true|     1000.0|
|false|     5000.0|
+-----+-----------+
```

#### Agg()

```scala
// agg 聚合操作
var df3 = List(
    (1, "zs", true, 1, 15000),
    (2, "ls", false, 2, 18000),
    (3, "ww", false, 2, 14000),
    (4, "zl", false, 1, 18000),
    (4, "zl", false, 1, 16000))
.toDF("id", "name", "sex", "dept", "salary")
import org.apache.spark.sql.functions._
df3.groupBy("sex")
    // .agg(max("salary"), min("salary"), avg("salary"), sum("salary"), count("salary"))
    .agg(Map(("salary", "max"))) // 另外的一种写法【局限性 只支持单个字段的聚合查询】
    .show()
//-----------------------------------------------------------------------------
+-----+-------------+
|  sex|count(salary)|
+-----+-------------+
| true|            1|
|false|            4|
+-----+-------------+
```

#### Limit()

```scala
// limit 限制返回的结果条数
df.limit(2).show()
//-----------------------------------------------------------------------------
+---+----+------+-----+
| id|name|salary|  sex|
+---+----+------+-----+
|  1|  zs|1000.0| true|
|  2|  ls|2000.0|false|
+---+----+------+-----+
```

#### Where()

```scala
val df4=List(
      (1,"zs",true,1,15000),
      (2,"ls",false,2,18000),
      (3,"ww",false,2,14000),
      (4,"zl",false,1,18000),
      (5,"win7",false,1,16000)).toDF("id","name","sex","dept","salary")
    df4.select($"id",$"name",$"sex",$"dept",$"salary")
      //where("(name like '%s%' and salary > 15000) or name = 'win7'")
      .where(($"name" like "%s%" and $"salary" > 15000) or $"name" ==="win7" ).show()
//--------------------------------------------------------------------------------------
+---+----+-----+----+------+
| id|name|  sex|dept|salary|
+---+----+-----+----+------+
|  2|  ls|false|   2| 18000|
|  5|win7|false|   1| 16000|
+---+----+-----+----+------+
```

#### Pivot() 【透视】

```scala
var scoreDF=
	List(
        (1,"math",85),
        (1,"chinese",80),
        (1,"english",90), 
        (2,"math",90), 
        (2,"chinese",80))
		.toDF("id","course","score")

scoreDF
      .groupBy($"id")
      .pivot($"course")  // 行转列【重点】
      .max("score")
      .show()

//--------------------------------------------------------------------------------------
+---+-------+-------+----+
| id|chinese|english|math|
+---+-------+-------+----+
|  1|     80|     90|  85|
|  2|     80|   null|  90|
+---+-------+-------+----+
```

#### na()

> 对空值的一种处理方式 
>
>    na().fill 填充  null赋予默认值
>
>    na().drop 删除为null的一行内容

```scala
scoreDF
      .groupBy($"id")
      .pivot($"course") // 行转列【重点】
      .max("score")
      //.na.fill(Map("english" -> 59))  // 为空值赋予一个默认值
      .na.drop()  // 删除包含空值的一行记录
      .show()
//--------------------------------------------------------------------------------------
+---+-------+-------+----+
| id|chinese|english|math|
+---+-------+-------+----+
|  1|     80|     90|  85|
+---+-------+-------+----+
```



#### over() 

>  窗口函数:
>
> - 聚合函数
> - 排名函数
> - 分析函数
>
> 作用： 窗口函数使用over，对一组数据进行操作，返回普通列和聚合列
>
> val w1 = Window
>
> ​	.partitionBy("分区规则")
>
> ​	.orderBy($"列" asc| desc)
>
> ​	.rangeBetween | rowsBetween
>
> 窗口函数名  over(w1)

```
t_user
	id    name   salary   sex   dept
	1     zs     1000     true   1
	2     ls	 2000	  false  2
	3     ww	 2000	  false  1

	// 查询用户信息（id,name,salary,用户所在部门的平均工资）
	
	SQL: select id,name,salary,(select avg(salary) from t_user group by dept) as avg_salary from t_user
	
	id   name salaray  avg_salary
	1    zs   1000     1500
	2	 ls   2000     2000
	3    ww   2000     1500
	
	spark sql 窗口函数 简化如上查询
	
	语义： select id,name,salary,avg(salary) over(partition by dept order by ...) from t_user
	
具体使用方法：
count(...) over(partition by ... order by ...) --求分组后的总数。
sum(...) over(partition by ... order by ...)   --求分组后的和。
max(...) over(partition by ... order by ...)--求分组后的最大值。
min(...) over(partition by ... order by ...)--求分组后的最小值。
avg(...) over(partition by ... order by ...)--求分组后的平均值。
rank() over(partition by ... order by ...)--rank值可能是不连续的。
dense_rank() over(partition by ... order by ...)--rank值是连续的。
first_value(...) over(partition by ... order by ...)--求分组内的第一个值。
last_value(...) over(partition by ... order by ...)--求分组内的最后一个值。
lag() over(partition by ... order by ...)--取出前n行数据。　　
lead() over(partition by ... order by ...)--取出后n行数据。
ratio_to_report() over(partition by ... order by ...)--Ratio_to_report() 括号中就是分子，over() 括号中就是分母。
percent_rank() over(partition by ... order by ...)
```

![1573178096543](assets/1573178096543.png)

```scala
//------------------------------------------------------------
val df2 = List(
    (1, "zs", true, 1, 15000),
    (2, "ls", false, 2, 18000),
    (3, "ww", false, 2, 14000),
    (4, "zl", false, 1, 18000),
    (5, "win7", false, 1, 16000))
.toDF("id", "name", "sex", "dept", "salary")

// 定义窗口函数
val w1 = Window
.partitionBy($"dept") // 根据部门dept进行分区： 部门相同的数据划分到同一个分区
	.orderBy($"salary" asc) // 对分区内的数据 按照工资salary进行降序排列
    // .rangeBetween(0, 2000) // 窗口数据可视范围  【基于数据范围】
	// .rowsBetween(0, 1) // 窗口数据可视范围
	.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing) // 窗口数据可视范围【基于行 使用行的偏移量】

import org.apache.spark.sql.functions._ // 导入隐式转换函数

df2.select($"id", $"name", $"sex", $"dept", $"salary")
.withColumn("sum_id", sum("id") over (w1))
.show()
```



#### Join()

```scala
val userInfoDF= sparkSql.sparkContext.makeRDD(List((1,"zs"),(2,"ls"),(3,"ww"))).toDF("id","name")
val orderInfoDF= sparkSql.sparkContext.makeRDD(List((1,"iphone",1000,1),(2,"mi9",999,1),(3,"连衣裙",99,2))).toDF("oid","product","price","uid")

// join DF连接操作
userInfoDF
    .join(orderInfoDF,$"id"===$"uid","inner")
    .show()
//-----------------------------------------------------------------
+---+----+---+-------+-----+---+
| id|name|oid|product|price|uid|
+---+----+---+-------+-----+---+
|  1|  zs|  1| iphone| 1000|  1|
|  1|  zs|  2|    mi9|  999|  1|
|  2|  ls|  3| 连衣裙|   99|  2|
+---+----+---+-------+-----+---+
```

#### cube(**多维度**) 

> cube多维度查询 尝试根据多个分组可能继续数据查询
>
> cube(A,B)
>
> ​		group by A  null 
>
> ​	   group by null B
>
> ​       group by null null
>
> ​	   group by AB



```scala
import org.apache.spark.sql.functions._
List(
    (110,50,80,80),
    (120,60,95,75),
    (120,50,96,70))
.toDF("height","weight","IQ","EQ")
.cube($"height",$"weight")  // spark sql尝试根据元组第一个和第二个值 进行各种可能分组操作，这种操作的好处，如果以后有任何第一个和第二个值的分区操作，都将出现在cube的结果表中
.agg(avg("IQ"),avg("EQ")) .show()
```



### DataSet操作（typed）

> 在实际开发中，我们通常使用的是DataFrame API，这种Dataset强类型的操作几乎不使用

```scala
package com.baizhi.sql.operation.typed

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.scalalang.typed

/**
  * ds 强类型操作
  *
  * spark context :rdd
  * spark streaming context : streaming
  * spark session : sql
  */
object DatasetWithTypedOpt {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("typed opt").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val ds = spark.sparkContext.makeRDD(List("Hello Hadoop", "Hello Scala")).flatMap(_.split(" ")).map((_, 1)).toDS

    ds
      .groupByKey(t => t._1) // 根据单词进行分组操作
      .agg(typed.sum[(String, Int)](t => t._2)) // 对单词初始值进行聚合
      .withColumnRenamed("TypedSumDouble(scala.Tuple2)", "num")
      .show()

    spark.stop()
  }
}
```



## 五、DataFrame纯SQL操作

```
df.createGlobalTempView()  //  对DF创建全局的临时视图，它产生的表，可以多个spark session共享，它的生命周期和spark application绑定
df.createTempView()  // 对DF创建局部的临时视图，它产生的表，仅供创建spark session使用，其它的spark session无法获取
```

### 单行查询

```scala
package com.baizhi.sql

import org.apache.spark.sql.SparkSession

object DataFrameSqlOpt {
  def main(args: Array[String]): Unit = {

    // 1. sparkSession是spark sql应用入口，内部封装了sparkconf和sparkContext
    val spark = SparkSession
      .builder()
      .appName("the first spark sql example")
      .master("local[*]")
      .getOrCreate()

    // 2. 创建Dataset
    val rdd = spark.sparkContext.makeRDD(List("Hello Hadoop", "Hello Scala")).flatMap(_.split(" ")).map((_, 1))

    import spark.implicits._

    val df = rdd.toDF("word", "num")

    // 给df起了表名 如果是全局表的话，在访问的时候需要加数据库名【】
    // df.createGlobalTempView("t_user") //  对DF创建全局的临时视图，它产生的表，可以多个spark session共享，它的生命周期和spark application绑定
    df.createTempView("t_user") // 对DF创建局部的临时视图，它产生的表，仅供创建spark session使用，其它的spark session无法获取


    // 再创建一个session，请问是否能够使用全局表？ 正确
    //    val newSparkSession = spark.newSession()
    //    spark.sql("select * from global_temp.t_user").show()
    //    newSparkSession.sql("select * from global_temp.t_user").show()

    // 再创建一个session，请问是否能够使用局部临时表？ 错误
    val newSparkSession = spark.newSession()
    spark.sql("select * from t_user").show()
    newSparkSession.sql("select * from t_user").show()
    spark.stop()
  }
}
```

### 模糊查询

```scala
import spark.implicits._
val userDF = List((1, "zs", true, 18, 15000, 1), (2, "ls", false, 19, 15000, 1)).toDF("id", "name", "sex", "age", "salary", "dept")

userDF.createTempView("t_user")

spark.sql("select * from t_user where name like '%z%' and age > 18").show()
```

### 排序查询

```scala
// 排序查询
spark.sql(
    // 自动将"""引起的内容 进行字符串拼接
    """
        select
          *
        from t_user
        order by id desc
    """).show()
```

### 分组查询

```scala
spark.sql(
      """
        select
          sex,avg(salary)
            as avg_salary
        from
          t_user
        group
          by sex
      """).show()
//---------------------------------------------------------------------------

+-----+----------+
|  sex|avg_salary|
+-----+----------+
| true|   15000.0|
|false|   15000.0|
+-----+----------+
```

### Limit

> 限制返回结果条数

```scala
// 分组查询  统计男和女的平均工资
spark.sql(
    """
        select
          sex,avg(salary)
            as avg_salary
        from
          t_user
        group
          by sex
        limit 1
      """).show()
//---------------------------------------------------------------------------
+----+----------+
| sex|avg_salary|
+----+----------+
|true|   15000.0|
+----+----------+
```



### having 分组后过滤

```scala
spark.sql(
    """
            select
              sex,avg(salary)
                as avg_salary
            from
              t_user
            group
              by sex
            having
              sex = true
          """).show()
//---------------------------------------------------------------------------
+----+----------+
| sex|avg_salary|
+----+----------+
|true|   15000.0|
+----+----------+
```



### Case ... when 语句

```scala
spark.sql(
      """
        | select
        |   id,name,salary,age,
        |     case sex
        |       when true
        |         then '男'
        |       when false
        |         then '女'
        |       else
        |         '中性'
        |     end
        |   as newSex
        | from
        |   t_user
      """.stripMargin).show()

//----------------------------------------------------------------------------
+---+----+------+---+------+
| id|name|salary|age|newSex|
+---+----+------+---+------+
|  1|  zs| 15000| 18|    男|
|  2|  ls| 15000| 19|    女|
|  3|  ww| 18000| 19|    女|
+---+----+------+---+------+

```

### Pivot(行转列)

```scala
// pivot
val scoreDF=List(
    (1, "语文", 100),
    (1, "数学", 100),
    (1, "英语", 100),
    (2, "数学", 79),
    (2, "语文", 80),
    (2, "英语", 100),
    (2, "英语", 120))
.toDF("id","course","score")
scoreDF.createOrReplaceTempView("t_course")

// 注意: 缺省的列会作为分组的依据
spark.sql(
    """
        | select
        |   *
        | from
        |   t_course
        | pivot(max(score) for course in('语文','数学','英语'))
        |
      """.stripMargin).show()

//----------------------------------------------------------------------------
+---+----+----+----+
| id|语文|数学|英语|
+---+----+----+----+
|  1| 100| 100| 100|
|  2|  80|  79| 120|
+---+----+----+----+
```

### Cube(多维度分组)

```scala
// cube (A,B)
    //    A null
    //    null B
    //    A B
    val df2 = List(
      (110, 50, 80, 80),
      (120, 60, 95, 75),
      (120, 50, 96, 70))
      .toDF("height", "weight", "uiq", "ueq")
    df2.createTempView("tt_user")

    spark.sql(
      """
        | select
        |   height,uiq,avg(uiq)
        | from
        |   tt_user
        | group by
        |   cube(height,uiq)
      """.stripMargin).show()

//-----------------------------------------------------------------
+------+----+-----------------+
|height| uiq|         avg(uiq)|
+------+----+-----------------+
|   120|null|             95.5|
|  null|  80|             80.0|
|  null|null|90.33333333333333|
|  null|  95|             95.0|
|   120|  95|             95.0|
|   110|null|             80.0|
|   110|  80|             80.0|
|   120|  96|             96.0|
|  null|  96|             96.0|
+------+----+-----------------+
```

### Join表连接查询

```scala
// join
val userInfoDF = spark.sparkContext.makeRDD(List((1, "zs"), (2, "ls"), (3, "ww"))).toDF("id", "name")
val orderInfoDF = spark.sparkContext.makeRDD(List((1, "iphone", 1000, 1), (2, "mi9", 999, 1), (3, "连衣裙", 99, 2))).toDF("oid", "product", "price", "uid")

userInfoDF.createTempView("ttt_user")
orderInfoDF.createTempView("t_order")

// inner  left_outer  right_outer full  cross
spark.sql(
    """
        | select
        |   *
        | from
        |   ttt_user t1
        | inner join
        |   t_order t2
        | on
        |   t1.id = t2.uid
      """.stripMargin).show()
```

### 子查询

> 类似于SQL的子查询

```scala
// 子查询
    val df =
      List(
        (1, "zs", true, 1, 15000),
        (2, "ls", false, 2, 18000),
        (3, "ww", false, 2, 14000),
        (4, "zl", false, 1, 18000),
        (5, "win7", false, 1, 16000)
      ).toDF("id", "name", "sex", "dept", "salary")
    df.createTempView("t_employee")

    
    spark.sql(
      """
        select
          id,
          name,
          sex,
          dept
        from (select * from t_employee)
      """.stripMargin).show()

//-----------------------------------------------------
+---+----+-----+----+
| id|name|  sex|dept|
+---+----+-----+----+
|  1|  zs| true|   1|
|  2|  ls|false|   2|
|  3|  ww|false|   2|
|  4|  zl|false|   1|
|  5|win7|false|   1|
+---+----+-----+----+
```

### 窗口函数

在正常的统计分析中 ，通常使用聚合函数作为分析，聚合分析函数的特点是将n行记录合并成一行，在数据库的统计当中 还有一种统计称为开窗统计，开窗函数可以实现将一行变成多行。可以将数据库查询的每一条记录比作是一幢高楼的一层, 开窗函数就是在每一层开一扇窗, 让每一层能看到整装楼的全貌或一部分。

> 语法：
>
> ​	`窗口函数名() over([partition by 分区字段] order by 字段 asc | desc [range | rows between unbounded preceding and unbounded following] )`

```scala
// 创建DF
    val df = List(
      (1, "zs", true, 1, 15000),
      (2, "ls", false, 2, 18000),
      (3, "ww", false, 2, 14000),
      (4, "zl", false, 1, 18000),
      (5, "win7", false, 1, 16000)).toDF("id", "name", "sex", "dept", "salary")
    df.createTempView("t_employee")
    // 	窗口函数名() over([partition by 分区字段] order by 字段 asc | desc [range | rows between unbounded preceding and unbounded following] )
    spark.sql(
      """
        | select
        |   id,name,sex,dept,salary,
        |   sum(id) over(partition by dept order by salary rows between unbounded preceding and unbounded following) as sum_id,
        |   sum(id) over(partition by dept order by salary) as sum_id2,
        |   sum(id) over() as sum_id3,
        |   sum(id) over(partition by dept order by salary rows between 1 preceding and 1 following) as sum_id4,
        |   sum(id) over(partition by dept order by salary range between 1000 preceding and 2000 following) as sum_id5,
        |   row_number() over(partition by dept order by salary) as rn,
        |   rank(salary) over(partition by dept order by salary) as salary_rank,
        |   dense_rank(salary) over(partition by dept order by salary asc) as salary_rank2,
        |   lag(salary,2) over(partition by dept order by salary asc) as lag2
        | from
        |   t_employee
      """.stripMargin).show()
    // 第一个表示 以任意的一行数据为基准都可以看到窗口的所有数据
    // 第二个表示 没有加任何的数据可视范围，使用默认的数据可视范围 rowsBetween[Long.min_value,0]
    // 第三个表示 over没有声明任何的窗口函数内容，则在每行显示整张表的聚合结果  // agg = 15
    // 第四个表示 以当前行为基准 上一行和下一行 rowsBetween[-1,1]
    // 第五个表示 数据可视范围区间 range between[当前数据排序字段为基准-下界,当前数据排序字段为基准+上界]
    // 第六个表示 排序窗口函数 row_number() 给窗口的数据添加一个序号 类似与Oracle伪列rownum
    // 第七个表示 对窗口函数 rank(排名字段) 给窗口的数据按照排名字段信息排名  注意： 非密集或者非连续的排名
    // 第八个表示 对窗口函数 dense_rank(连续密集排名字段) 给窗口的数据按照排名字段信息排名  注意： 密集或者连续的排名
    // 第九个表示 获取往上两行的slary的值 作为当前行窗口的值
	spark.stop()

//--------------------------------------------------------------------------------
+---+----+-----+----+------+------+-------+-------+-------+-------+---+-----------+------------+-----+
| id|name|  sex|dept|salary|sum_id|sum_id2|sum_id3|sum_id4|sum_id5| rn|salary_rank|salary_rank2| lag2|
+---+----+-----+----+------+------+-------+-------+-------+-------+---+-----------+------------+-----+
|  1|  zs| true|   1| 15000|    23|      1|     28|      6|      6|  1|          1|           1| null|
|  5|win7|false|   1| 16000|    23|      6|     28|     10|     16|  2|          2|           2| null|
|  4|  zl|false|   1| 18000|    23|     16|     28|     15|     17|  3|          3|           3|15000|
|  6|  wb|false|   1| 18000|    23|     16|     28|     17|     17|  4|          3|           3|16000|
|  7| wb2|false|   1| 20000|    23|     23|     28|     13|      7|  5|          5|           4|18000|
|  3|  ww|false|   2| 14000|     5|      3|     28|      5|      3|  1|          1|           1| null|
|  2|  ls|false|   2| 18000|     5|      5|     28|      5|      2|  2|          2|           2| null|
+---+----+-----+----+------+------+-------+-------+-------+-------+---+-----------+------------+-----+
```

> unbounded preceding  等价于 Long.min_value
>
> unbounded  following   等价于 Long.max_value
>
> current row： 当前行
>
> current row  - 1 ：　当前行的上一行
>
> current row  ＋1　：　当前行的下一行



## 六、Spark SQL的自定义函数

### 单行函数

> 对每一行数据应用函数内容，如：Upper() Lower()  Length()

```scala
package com.baizhi

import org.apache.spark.sql.SparkSession

/**
  *
  */
object CustomUserSingleFunction1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("window function").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // 创建DF
    val df = List(
      (1, "zs", true, 1, 15000),
      (2, "ls", false, 2, 18000),
      (3, "ww", false, 2, 14000),
      (4, "zl", false, 1, 18000),
      (5, "win7", false, 1, 16000),
      (6, "wb", false, 1, 18000),
      (7, "wb2", false, 1, 20000)
    ).toDF("id", "name", "sex", "dept", "salary")
    df.createTempView("t_employee")

    // 自定义单行函数
    spark.udf.register("sex_converter", (sex: Boolean) => {
      sex match {
        case true => "男"
        case false => "女"
        case _ => "不男不女"
      }
    })

    spark.sql("select id,upper(name),sex_converter(sex) from t_employee").show()

    spark.stop()
  }
}
//----------------------------------------------------------------
+---+-----------+----------------------+
| id|upper(name)|UDF:sex_converter(sex)|
+---+-----------+----------------------+
|  1|         ZS|                    男|
|  2|         LS|                    女|
|  3|         WW|                    女|
|  4|         ZL|                    女|
|  5|       WIN7|                    女|
|  6|         WB|                    女|
|  7|        WB2|                    女|
+---+-----------+----------------------+
```

### 多行函数

> 指对多行数据应用函数内容，返回单个结果. 如：聚合函数 sum()  avg()  min()...
>
> 需求：自定义   整数求和的多行函数

```scala
package com.baizhi

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

/**
  *
  */
object CustomUserSingleFunction2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("window function").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // 创建DF
    val df = List(
      (1, "zs", true, 1, 15000),
      (2, "ls", false, 2, 18000),
      (3, "ww", false, 2, 14000),
      (4, "zl", false, 1, 18000),
      (5, "win7", false, 1, 16000),
      (6, "wb", false, 1, 18000),
      (7, "wb2", false, 1, 20000)
    ).toDF("id", "name", "sex", "dept", "salary")
    df.createTempView("t_employee")

    // 自定义多行函数
    spark.udf.register("my_sum", new UserDefinedAggregateFunction {

      /**
        * 输入数据的结构类型
        *
        * @return
        */
      override def inputSchema: StructType = new StructType().add("salary", IntegerType)

      /**
        * 缓冲区【用来存放聚合产生的临时结果】的结构类型
        *
        * @return
        */
      override def bufferSchema: StructType = new StructType().add("total", IntegerType)

      /**
        * 聚合操作结束后的返回值类型
        *
        * @return
        */
      override def dataType: DataType = IntegerType

      /**
        * 聚合操作时，输入类型和聚合结果的返回类型是否匹配
        *
        * @return
        */
      override def deterministic: Boolean = true

      /**
        * 初始化方法
        *
        * @param buffer
        */
      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // buffer缓冲区的第一个位置 存放了一个初始值0
        buffer.update(0, 0)
      }

      /**
        * 修改方法
        *
        * @param buffer
        * @param input
        */
      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 获取多行函数的第一个参数的值
        val rowValue = input.getInt(0)
        val currentValue = buffer.getInt(0)
        buffer.update(0, rowValue + currentValue)
      }

      /**
        * 合并
        * 将两个buffer中的数据合并 并将最终结果保存到第一个buffer中
        *
        * @param buffer1
        * @param buffer2
        */
      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val b1CurrentValue = buffer1.getInt(0)
        val b2CurrentValue = buffer2.getInt(0)
        buffer1.update(0, b1CurrentValue + b2CurrentValue)
      }

      /**
        * 评估方法  返回聚合结果
        *
        * @param buffer
        * @return
        */
      override def evaluate(buffer: Row): Any = buffer.getInt(0)
    })

    spark.sql("select my_sum(salary) from t_employee").show()

    spark.stop()
  }
}
//------------------------------------------------------------------------------------------
+--------------+
|anon$1(salary)|
+--------------+
|        119000|
+--------------+
```



## 七、Spark SQL的数据导入导出

> 指的是和第三方存储系统的读写操作

### JSON

```scala
val df = spark.read.json("file:///G:\\IDEA_WorkSpace\\scala-workspace\\spark-day9\\src\\main\\resources")

df.createTempView("t_user")

spark.sql("select id,name from t_user").write.format("json").save("file:///D://result")
```

### Paquet

> 基于列式存储的文件格式，底层会将数据编码成二进制数据
>
> 【默认】

```scala
// 读parquet文件内容 创建df
val df = spark.read.parquet("file:///D://result2")

// 写出parquet文件格式
spark.sql("select id,name from t_user").write.save("file:///D://result2")
```

### ORC

> 矢量化文件格式，比较节省磁盘空间

```scala
// 写出orc文件格式
spark.sql("select * from t_user").write.orc("file:///d://result3")

// 读orc文件内容 创建df
val df = spark.read.orc("file:///D://result3")
```

### CSV

```scala
// 写出CSV文件格式
df.write
    .format("csv")
    .option("sep", ",")
    .option("inferSchema", "true")
    .option("header", "true")
    .save("file:///D://result4")

// 读csv文件内容 创建df
    val df = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("file:///d://result4")
```

### JDBC

```scala
// 写出JDBC Mysql数据库中
spark
    .sql("SELECT * FROM t_user")
    .write
    .format("jdbc")
    .mode(SaveMode.Overwrite) // 覆盖
    .option("user", "root")
    .option("password", "1234")
    .option("url", "jdbc:mysql://hadoopnode00:3306/test")
    .option("dbtable", "t_user")
    .save()

// 读JDBC Mysql中数据，创建DF
    val df = spark
      .read
      .format("jdbc")
      .option("user", "root")
      .option("password", "1234")
      .option("url", "jdbc:mysql://hadoopnode00:3306/test")
      .option("dbtable", "t_user").load()
```

### DF转RDD

```scala
// df转rdd
df.rdd.foreach(row => {
    println(row.getInt(0) + "\t" + row.getString(1)) // row中获取元素的方法 内容为下标
})
```



> 总结：Spark SQL 建立在SparkRDD基础之上的一个通过SQL语法进行计算的引擎工具





 