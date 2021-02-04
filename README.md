# Spark Exercises

PySpark scripts to solve some Big Data Problems as part of the Cloud and Big Data subject.

To run the exercises it is required to have Spark installed in your machine (this also require Java, Python and Scala installations).

For each exercise we include the spark script, the input file and an output example.

The exercises included are:
### Excercise 1: Distributed Grep
Given a word, the program should output the lines of the 'input.txt' document that contains it.
- Spark script: [`P1_spark.py`](P1_spark.py)
- Input File: [`input.txt`](input.txt)
- Output Directory: [`output1`](output1)
- Test Command: `spark-submit P1_spark.py flat`

### Excercise 2: URL frequency
Find the frecuency of each URL ina  sample Apache log file.
- Spark script: [`P2_spark.py`](P2_spark.py)
- Input File: [`access_log`](access_log)
- Output directory: [`output2`](output2)
- Test Command: `spark-submit P2_spark.py`

### Excercise 3: Stock Summary
Calculate the average daily stock price at close of Alphabet Inc. (GOOG) per year since 2009. The fields in the .csv file are: Date, Open, High, Low,	Close, Adj Close, Volume
- Spark script: [`P3_spark.py`](P3_spark.py)
- Input File: [`GOOGLE.csv`](GOOGLE.csv)
- Output Directory: [`output3`](output3)
- Test Command: `spark-submit P3_spark.py`

### Excercise 4: Movie Rating
Show a range of average ratings and the movies that belong to it: 
Range 1: 1 or lower
Range 2: 2 or lower (but higher than 1)
Range 3: 3 or lower (but higher than 2)
Range 4: 4 or lower (but higher than 3)
Range 5: 5 or lower (but higher than 4)

- Spark script: [`P4_spark.py`](P4_spark.py)
- Input File: [`ratings.csv`](ratings.csv)
- Output Directory: [`output4`](output4)
- Test Command: `spark-submit P4_spark.py`

### Excercise 5: Meteorite Landing
Calculate the average mass per meteorite class. You have to clean de data. The columns needed are the thir one (meteroite class) and the fourth (meteorite mass).
- Spark script: [`P5_spark.py`](P5_spark.py)
- Input File: [`Meteorite_Landings.csv`](Meteorite_Landings.csv)
- Output Directory: [`output5`](output5)
- Test Command: `spark-submit P5_spark.py`


