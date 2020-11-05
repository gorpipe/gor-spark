# gor-spark
Spark enabled GOR

GOR scalable through the Spark engine (https://spark.apache.org)

# Checkout and build SparkGOR
```bash 
git clone git@github.com:gorpipe/gor-spark.git
cd gor-spark
./gradlew clean installDist
```

## Usage
Now you can use SparkSQL from within GOR
```bash
spark/build/install/gor-scripts/bin/gorpipe "select * from genes.gor limit 10"
spark/build/install/gor-scripts/bin/gorpipe "create xxx = select * from <(select * from genes.gor) where Gene_Symbol like 'B%'; gor [xxx] | top 10"
```

## SDK usage
#### Scala demo: [gorspark.scala](https://gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark/-/blob/master/pyspark.scala)
```bash
spark-shell --packages org.gorpipe:gor-spark:2.11.10 -I gorspark.scala
```
#### Python demo: [gorspark.py](https://gitlab.com/wuxi-nextcode/wxnc-gor/gor-spark/-/blob/master/pyspark.py)
```bash
pyspark --packages org.gorpipe:gor-spark:2.11.10 -I gorspark.py
```