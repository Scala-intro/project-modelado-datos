import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column


object project {
  def main (args: Array[String]) : Unit ={
    val spark = SparkSession.builder()
      .appName("Project")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    //Carga y Limpieza de Datos
    val socialMediaDF = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("dataSentiment/sentimentdataset.csv")

    // Limpieza de Datos
    socialMediaDF.select(socialMediaDF.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)):_*).show()
    //Análisis Exploratorio y Transformaciones

    val sentimentCounts = socialMediaDF.groupBy("sentiment").count()
    sentimentCounts.show()


    val dateFormattedDF = socialMediaDF.withColumn("date",to_date(col("timestamp")))


    // Distribución de Sentimientos

    val sentimentDistribution = dateFormattedDF.groupBy("Sentiment")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))
    sentimentDistribution.show()

    // Frecuencia de Palabras por Sentimiento

    val stopwords = Set("the", "and", "to", "a", "is", "in", "for", "on")

    // Separar el texto en palabras y convertirlas a minúsculas
    val wordsDF = dateFormattedDF.withColumn("word",explode(split(lower(col("Text")), " ")))

    // Filtrar las palabras vacías (stopwords)
    val filteredWordsDF = wordsDF.filter(!col("word").isin(stopwords.toSeq: _*))

    //Contar las palabras por sentimiento
    val wordCountDF = filteredWordsDF.groupBy("Sentiment","word")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))
    wordCountDF.show(20)
    // Tendencias Temporales de Sentimientos
    val dfTime = dateFormattedDF.withColumn("Year",year(col("Timestamp")))
      .withColumn("Month",month(col("Timestamp")))
      .withColumn("Day",dayofmonth(col("Timestamp")))

    val sentimentTrendDF = dfTime.groupBy("Year","Month","Day","Sentiment")
      .agg(count("*").alias("count"))
      .orderBy("Year","Month","Day")
    sentimentTrendDF.show()

    // Hashtags Más Populares por Sentimiento

    val hashtagsDF = dfTime.withColumn("Hashtags",explode(split(col("Hashtags")," ")))
      .filter(col("Hashtags").startsWith("#"))
      .groupBy("Hashtags")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))



    hashtagsDF.show(20)
    // Procesamiento Batch con Parquet
    """
    dateFormattedDF
      .coalesce(4)
      .write
      .mode("overwrite")
      .option("compression","snappy")
      .parquet("dataSentiment/social_media_parquet")

    """
    spark.stop()
  }
}