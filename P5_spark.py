# Javier Guzmán Muñoz

from pyspark.sql import SparkSession
import shutil

# Set up Spark in local mode.
spark = SparkSession.builder.appName('MeteoriteLanding').master('local').getOrCreate()

# Create dataFrame from csv input file, in this case we don't have a line with the columns names.
df = spark.read.csv('Meteorite_Landings.csv')

# Remove the directory where results will be stored, if it exits.
shutil.rmtree('output5', ignore_errors = True, onerror = None)

# Rename the columns corresponding to the meteorite class and mass and create a new dataFrame
# by selecting them.
df2 = (df.withColumnRenamed(df.columns[3], 'recclass')
	.withColumnRenamed(df.columns[4], 'mass')
	.select('recclass', 'mass'))

# We clean the data, filtering to create a new dataFrame only with the rows where mass and class aren't
# null and the mass is greater than 0 (a meteorite with zero mass doesn't make sense).
(df2.filter(df2.mass.isNotNull() & df2.recclass.isNotNull()).filter(df2['mass'] > 0)

	# Group by class, adding a column with the average mass for the observations of that class.
	.groupBy('recclass').agg({'mass': 'avg'})

	# Sort by class (to show data ordered, this is NOT necessary)
	.sort('recclass')

	# In order to store the result, we transform the RDD into a dataFrame and reduce the number of
	# partitions to one, so the data will appear together in a single file.
	.rdd.coalesce(1).saveAsTextFile('output5'))
