# Javier Guzmán Muñoz

from pyspark import SparkConf, SparkContext
import sys
import string
import re
import shutil

# Set up Spark in local mode.
conf = SparkConf().setMaster('local').setAppName('URL_Frequency')
sc = SparkContext(conf = conf)

# Remove the directory where results will be stored, if it exists.
shutil.rmtree('output2', ignore_errors=True, onerror=None)

# Create RDD from input file 'access_log'.
(sc.textFile('access_log')

	# Create a  new RDD only with the second part of the line if we split by '"', where we have
	# the command (GET, POST, OPTIONS) and URLs can appear. Then, we split by ' ', because if we have
	# an URL the structure should be "COMMAND URL PROTOCOL".
	.map(lambda line : line.split('\"')[1].split(" "))

	# Create a new RDD with the lines in which this last split has resulted in three parts.
	.filter(lambda x: len(x) == 3)

	# Create a new RDD with the result of searching a URL in each line.
	.map(lambda x: re.search(r"/\S+",x[1]))

	# Filter to create a new RDD with the results of searching URLs that are not null.
	# (That is, the new RDD contains the URLs)
	.filter(lambda x: x!= None)

	# Create a new RDD with tuples (URL, 1).
	.map(lambda x: (x.group(0), 1))

	# Group equal URL and count its frequency.
	.reduceByKey(lambda x, y: x+y)

	# Store results in the directoy 'output2'
	.saveAsTextFile('output2'))

