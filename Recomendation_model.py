from pyparsing import col
from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *

# setting up configurations

conf = SparkConf().set("spark.driver.memory", "6g").setAppName("movie_recommendation")\
    .set("spark.sql.warehouse.dir","/home/sunbeam/MyWork/spark/private/warehouse")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Loading ratings file:
# creating the model

"""
Schema of ratings:
|-- userId: integer (nullable = true)
 |-- movieId: integer (nullable = true)
 |-- rating: double (nullable = true)
 |-- timestamp: integer (nullable = true)
"""
ratings = spark.read\
    .option("delimiter",",")\
    .option("header",'true')\
    .option("inferSchema", 'true')\
    .csv('/home/sunbeam/dbda/spark/data/ratings.csv')

# Loading movies file file:

"""
Schema of movies file:
 |-- movieId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- genres: string (nullable = true)
"""

movies = spark.read\
    .option("delimiter","^")\
    .option("header","true")\
    .option("inferSchema","true")\
    .csv("/home/sunbeam/dbda/hadoop/data/movies/movies_caret.csv")

movies.printSchema()


#creating alias for self join operations
rating_table1 = ratings.alias("rating_table1")
rating_table2 = ratings.alias("rating_table2")

user_ratings = rating_table1.join(rating_table2,"userId")\
    .where("rating_table1.movieId < rating_table2.movieId")\
    .selectExpr("rating_table1.userId AS id","rating_table1.movieId  AS movie1","rating_table2.movieId AS movie2", "rating_table1.rating as rating1","rating_table2.rating AS rating2")\
    .withColumn("m1m2",concat("movie1" ,lit("-"),"movie2"))

# user_ratings.show()

"""
calculating the correlation between movies(movies relation are between the same user) 
"""
corr_table = user_ratings.groupBy("m1m2")\
    .agg(corr(col("rating1"),col("rating2")).alias("corr"), count(col("m1m2")).alias("cnt"))\
    .withColumn("movie1", split("m1m2", "-")[0])\
    .withColumn("movie2", split("m1m2", "-")[1])\
    .where("corr is not null and cnt > 1")\
    .drop("m1m2")

# corr_table.show()

corr_table.write.saveAsTable("corr_table")
movies.write.saveAsTable("movies_title")

