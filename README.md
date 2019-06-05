# movieRecommendation-pyspark
small assignment for creating movie recommendation system using spark and python
Simple programming is done using spark dataframes to create a small basic recommendation system as POC for Spark Dataframes

In Recommendation_model.py : Model is created where correlation between the movies are calculated from the ratings.csv file and stored in a spark table for recommendation

In Recommend_movie.py , the above table that is created is used with the movies_carot.csv table to find the top 10 related movies for a given movie id from the user. 
