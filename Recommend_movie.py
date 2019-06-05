#grtting top 10 movies recommended from the input movie number

from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *

conf = SparkConf().set("spark.driver.memory", "6g").setAppName("movie_recommendation")
spark = SparkSession.builder.config(conf=conf).getOrCreate()


corrThresold = 0.7
countThresold = 3

"""
correlation table
 |-- corr: double (nullable = true)
 |-- cnt: long (nullable = true)
 |-- movie1: string (nullable = true)
 |-- movie2: string (nullable = true)

movie table
 |-- movieId: integer (nullable = true)
 |-- title: string (nullable = true)

"""

corr_table = spark.read.parquet("/home/sunbeam/MyWork/spark/private/warehouse/corr_table/*")
movie_table = spark.read.parquet("/home/sunbeam/MyWork/spark/private/warehouse/movies_table/*")

#
# corr_table.printSchema()
# movie_table.printSchema()

movie_id =int(input("Enter the movie id :"))

related_MovieIds = corr_table\
    .where("(movie1={} or movie2={}) and corr > {} and cnt > {}".format(movie_id, movie_id, corrThresold, countThresold))\
    .orderBy(desc("corr"),desc("cnt"))\
    .limit(10)

# related_MovieIds = corr_table\
#     .orderBy(desc("corr"),desc("cnt"))\
#     .limit(50)

inputMovie = movie_table.select("title").where("movieId == {}".format(movie_id))

topMovies = movie_table.join(related_MovieIds,(related_MovieIds["movie1"] == movie_table["movieId"])|(related_MovieIds["movie2"] == movie_table["movieId"]))\
    .select("movieId","title").distinct()
    # .where("movie1 != movie_id")

finalList = topMovies.where("movieId != {}".format(movie_id))

# related_MovieIds.show()
# topMovies.show()
print("The input movie is : ")
inputMovie.show(truncate=False)
print(" ")
print("Recommended movies are:")

finalList.show(truncate=False)


