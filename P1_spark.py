# Javier Guzmán Muñoz

from pyspark import SparkConf, SparkContext
import sys
import shutil
import string

# Set up spark in local mode.
conf = SparkConf().setMaster('local').setAppName('Grep')
sc = SparkContext(conf = conf)

# The word that we are searching for.
word = sys.argv[1]

# Remove the directory where results will be stored, if it exists.
# (If we don't do that, it will raise an error everytime the spark script is
# executed if we have forgotten to remove the directory before, so by adding this line
# we don't have to care about that).
shutil.rmtree('output1', ignore_errors=True, onerror=None)

# Load RDD from input file.
(sc.textFile('input.txt')

	# Filter to create a new RDD only with the lines that contain the word we are searching for
	.filter(lambda line: word.lower() in line.replace('.', ' ').replace(',', ' ').lower().split(' '))

	# Save the results in the directory 'output1'
	.saveAsTextFile('output1'))


