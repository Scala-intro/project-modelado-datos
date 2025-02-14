import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column


object project_etl {
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
      .withColumn("Sentiment",trim(lower(col("Sentiment"))))

    println("dateformated")
    dateFormattedDF.show()


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

    dateFormattedDF.select("Likes","Retweets").describe().show()
    // Calcular percentiles 95 para Likes y Retweets

    val percentiles = dateFormattedDF.select(
      expr("percentile_approx(Likes, 0,95)").alias("likes_95"),
      expr("percentile_approx(Retweets, 0,95)").alias("retweets_95")
    ).collect()(0)

    val likes95 = percentiles.getAs[Double]("likes_95")
    val retweets95 = percentiles.getAs[Double]("retweets_95")

    val outliersPercentileDF = dateFormattedDF.filter(col("Likes") > likes95 || col("Retweets") > retweets95)
    outliersPercentileDF.show(10)
    val outliersCount = outliersPercentileDF.count()
    println(s"Cantidad de outliers detectados: $outliersCount")

    //  Método de Desviación Estándar (Outliers por Z-Score)

    val stats = dateFormattedDF.select(
      mean("Likes").alias("mean_likes"),
      stddev("Likes").alias("stddev_likes"),
      mean("Retweets").alias("mean_retweets"),
      stddev("Retweets").alias("stddev_retweets")
    ).collect()(0)


    val meanLikes = stats.getAs[Double]("mean_likes")
    val stddevLikes = stats.getAs[Double]("stddev_likes")
    val meanRetweets = stats.getAs[Double]("mean_retweets")
    val stddevRetweets = stats.getAs[Double]("stddev_retweets")
    val zScoreDF =dateFormattedDF.withColumn("Z_Likes", (col("Likes") - meanLikes) / stddevLikes)
      .withColumn("Z_Retweets", (col("Retweets") - meanRetweets) / stddevRetweets)

    // Filtrar outliers con |Z| > 2
    val outliersZScoreDF = zScoreDF.filter(abs(col("Z_Likes")) > 2 || abs(col("Z_Retweets")) > 2)

    //  Mostrar resultados
    println("📊 Datos con Z-Score calculado:")
    zScoreDF.show()

    println("🚨 Outliers detectados:")
    outliersZScoreDF.show()

    val sentimentByDateDF = dateFormattedDF
      .groupBy("Date", "Sentiment")
      .count()
      .orderBy("Date")

   sentimentByDateDF.show(10)

    val eventDate = "2023-02-14"

    val sentimentOnEventDF = sentimentByDateDF
      .filter(col("Date") === eventDate)
      .orderBy("Sentiment")

    // sentimentOnEventDF.show()

    val sentimentCountDF = dateFormattedDF
      .groupBy("Year","Month","Sentiment")
      .agg(count("*").as("Count"))

    val sentimentAvgDF = sentimentCountDF
      .groupBy("Year","Month","Sentiment")
      .agg(avg("count").as("AverageCount"))
      .orderBy("Year","Month")
    sentimentAvgDF.show()

    // Análisis Geográfico

    val sentimentByCountryDF = dateFormattedDF
      .groupBy("Country","Sentiment")
      .count()
      .orderBy(col("Country"),desc("count"))

    sentimentByCountryDF.show(5)

    // Relación entre Popularidad y Sentimiento

    val popularSentimentDF = dateFormattedDF
      .groupBy("Sentiment")
      .agg(
        avg("Likes").alias("avg_likes"),
        avg("Retweets").alias("avg_retweets")
      )
      .orderBy(desc("avg_likes"))
    popularSentimentDF.show()

    // Optimización del Pipeline

    dateFormattedDF
      .coalesce(4)
      .write
      .mode("overwrite")
      .option("compression","snappy")
      .parquet("dataSentiment/social_media_parquet")


    spark.stop()
  }
}