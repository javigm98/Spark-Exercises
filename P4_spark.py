# Javier Guzmán Muñoz

from pyspark.sql import SparkSession
from pyspark.sql.functions import ceil, collect_list, avg
import shutil

# Set up Spark in local mode.
spark = SparkSession.builder.appName('MovieRatings').master('local').getOrCreate()

# Create a DataFrame from csv file, considreing the first line as the column names, so the input
# file must contain this line.
df = spark.read.csv('ratings.csv', header = True)

# Remove directory where results will be stored.
shutil.rmtree('output4', ignore_errors = True, onerror = None)

# Select the two columns needed in the exercise.
(df.select('movieId', 'rating')

	# Change rating type to float.
	.withColumn('rating', df['rating'].cast('float'))

	# Group by movie id, calculating the average of the ratings.
	.groupBy('movieId').agg(avg('rating'))

	# Create a new column with the range corresponding to each film, which is the ceiling of
	# its average rating.
	.withColumn('Range', ceil('avg(rating)'))

	# We select only the range and the movie id.
	.select('Range', 'movieId')

	# We goup films by range, collecting in a list all movie ids in a range.
	.groupBy('Range').agg(collect_list('movieId').alias('ids'))

	# Sort by range (This is done to see results more clear)..
	.sort('Range')

	# We create a RDD from the dataFrame, and reduce the partitions number to one in order
	# to store results in a single file (if we don't do that it creates 199 output files).
	.rdd.coalesce(1).saveAsTextFile('output4'))

