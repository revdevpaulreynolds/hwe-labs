from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

### Setup: Create a SparkSession
spark = SparkSession.builder.appName("Week2").master("local").getOrCreate()

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

# Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv("resources/reviews.tsv.gz", header=True, sep="\t")

# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("reviews")
reviews_show = spark.sql("SELECT * FROM reviews")
# reviews_show.show(1)

# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
reviews_show_with_timestamp = reviews_show.withColumn("review_timestamp", current_timestamp())

# Question 4: How many records are in the reviews dataframe? 
# total_records = spark.sql("SELECT count(*) FROM reviews") 
# print("Total records in reviews dataframe: " + str(total_records.collect()[0][0]))
# totalReviews = reviews_show.select("*").count()
# print(totalReviews)
# 145431

# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
# first_5_rows = spark.sql("SELECT * FROM reviews LIMIT 5")
# for row in first_5_rows.collect():
#     print(row)

# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?
# product_category = spark.sql("SELECT DISTINCT product_category FROM reviews LIMIT 50")
# product_category.show()
# Digital Video Games!

# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
# highestHelpfulVotes = spark.sql("SELECT product_title, helpful_votes FROM reviews ORDER BY helpful_votes DESC LIMIT 1")
# highestHelpfulVotes.show(truncate=False)

# Question 8: How many reviews exist in the dataframe with a 5 star rating?
# five_star_reviews = spark.sql("SELECT count(*) FROM reviews WHERE star_rating = 5")
# five_star_reviews.show()

# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
# Look at 10 rows from this dataframe.
# star_rating|helpful_votes|total_votes
# reviews_with_ints = spark.sql("SELECT CAST(star_rating AS INT) as star_rating, CAST(helpful_votes AS INT) as helpful_votes, CAST(total_votes AS INT) as total_votes FROM reviews LIMIT 10")
# reviews_with_ints.show()

# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.
# date_with_most_purchases = spark.sql("SELECT purchase_date, count(*) as total_purchases FROM reviews GROUP BY purchase_date ORDER BY total_purchases DESC LIMIT 1")
# date_with_most_purchases.show()

##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.
reviews_show_with_timestamp.write.mode("overwrite").json("C:\\Users\\PaulReynolds\\Documents\\repos\\1904\\hwe-labs\\week2_sql\\reviews.json")

### Teardown
# Stop the SparkSession
spark.stop()