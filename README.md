# sparkgor
Spark enabled GOR

GOR scalable through the Spark engine (https://spark.apache.org)

## Prerequisite

Check out open source GOR 
```bash 
git clone git@github.com:sigmarkarl/gor.git
```

Checkout SparkGOR in the same parent directory
```bash 
git clone git@github.com:sigmarkarl/sparkgor.git
```

Checkout project glow (for Spark version 3) in the same parent directory
```bash 
git clone git@github.com:sigmarkarl/glow.git
```

### Build Glow project
```bash
cd glow
sbt package
```

## Build SparkGOR
```bash
cd sparkgor
./gradlew clean installDist
```

## Usage
Now you can use SparkSQL from within GOR
```bash
gorpipe "spark select * from genes.gor limit 10"
gorpipe "create xxx = spark select * from <(spark selec * from genes.gor) where Gene_Symbol like 'B%'; gor [xxx] | top 10"
```
