# Javier Guzmán Muñoz

from pyspark.sql import SparkSession
import shutil

# Set up Spark in local mode.
spark = SparkSession.builder.appName('StockSummary').master('local').getOrCreate()

# We create a dataFrame from the the csv file 'GOOGLE.csv', considering the first line of the file
# as the column names, so the file must contain this first line.
df = spark.read.csv('GOOGLE.csv', header = True)

# Remove the directory where results will be stored.
shutil.rmtree('output3', ignore_errors = True, onerror = None)

# We add a new column with the year of each row in the dataframe.
(df.withColumn('Year', df['Date'][0:4])

	# Change the type of 'Close' column to float, in order to later calculate the average
	# of these values.
	.withColumn('Close', df['Close'].cast('float'))

	# New DataFrame only with the two columns that are needed in the exercise.
	.select('Year', 'Close')

	# Group rows by year and calculate the average of the values corresponding to the close price.
	.groupBy('Year').agg({'Close': 'avg'})

	# Sort by year (this is not necessary, but clarifies results)
	.sort('Year')

	# We transform the dataFrame into a RDD and save the data in the directory 'output3'. With coalesce(1)
	# we group all task partitions into a single one, so results will be stored in a single file.
	.rdd.coalesce(1).saveAsTextFile('output3'))


