package sparkreeltime

import org.apache.spark.rdd.RDD
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable

object RealTimeRecom {
  // Case class to represent recommendations
  case class Recommendation(userID: Int, stationName: String, rating: Float)

  // API key for JCDecaux API
  val apiKey = "a6f35550996385e4ba9772f2c24e787f870351aa"

  // Configuring log4j for logging
  PropertyConfigurator.configure(getClass.getClassLoader.getResource("log4j.properties"))
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  // Fetch station data from the JCDecaux API
  val stationData: mutable.HashMap[Int, String] = fetchStationData(apiKey)

  // Function to fetch station data from the API and parse the response
  def fetchStationData(apiKey: String): mutable.HashMap[Int, String] = {
    val stationData = mutable.HashMap[Int, String]()
    val httpClient = HttpClients.createDefault()
    try {
      val getRequest = new HttpGet(s"https://api.jcdecaux.com/vls/v3/stations?apiKey=$apiKey")
      val response = httpClient.execute(getRequest)
      val entity = response.getEntity

      if (entity != null) {
        val content = EntityUtils.toString(entity, "UTF-8")
        logger.info(s"API response: $content")

        implicit val formats: DefaultFormats.type = DefaultFormats
        val parsedJson = parse(content)

        val stations = parsedJson.extract[List[JValue]]
        stations.foreach { station =>
          val stationIdOpt = (station \ "number").extractOpt[Int]
          val stationNameOpt = (station \ "name").extractOpt[String]

          (stationIdOpt, stationNameOpt) match {
            case (Some(stationId), Some(stationName)) =>
              stationData.put(stationId, stationName)
              logger.info(s"Station added: $stationId - $stationName")
            case _ =>
              logger.warn(s"Missing station number or name in station data: $station")
          }
        }

        // Adding a default station manually for demo purposes
        stationData.put(999, "Tunis")
        logger.info(s"Fetched ${stationData.size} stations from the API.")
      } else {
        logger.error("No response from API")
      }
    } catch {
      case e: Exception => logger.error(s"An error occurred while fetching station data: ${e.getMessage}", e)
    } finally {
      httpClient.close()
    }

    stationData
  }

  // Function to parse input data from a string line
  def parseInput(line: String): (Int, Int, Float) = {
    val fields = line.split("\\s")
    (fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }

  // Buffer to store recommendations
  var recommendationsBuffer = mutable.ListBuffer.empty[Recommendation]

  // Function to display recommendations and send them to Kafka
  def displayRecommendations(userID: Int, recommendations: Array[Rating], stationData: mutable.HashMap[Int, String], producer: KafkaProducer[String, String]): Unit = {
    logger.info(s"\nTop 5 recommendations for user $userID:")
    recommendations.foreach { case Rating(_, stationID, rating) =>
      if (rating > 0.0) {
        val stationName = stationData.getOrElse(stationID, "Unknown station")
        val recommendationMessage = s"User $userID, $stationName, Rating: $rating"
        val record = new ProducerRecord[String, String]("recommendations_topic", recommendationMessage)
        producer.send(record)
        logger.info(recommendationMessage)
        recommendationsBuffer += Recommendation(userID, stationName, rating.toFloat)

        println(recommendationMessage)
      }
    }
    logger.info(s"Current content of recommendationsBuffer: $recommendationsBuffer")
  }

  // Function to evaluate the ALS model
  def evaluateModel(recommendations: Array[Rating], testData: RDD[Rating]): Double = {
    val testUserProducts = testData.map { case Rating(user, product, _) =>
      (user, product)
    }
    val predictions = recommendations.map { case Rating(user, product, rating) =>
      ((user, product), rating)
    }
    logger.info(predictions)
    val spark = SparkSession.builder().appName("Real-Time Recommendations").master("local[*]").getOrCreate()
    val predictionsRDD = spark.sparkContext.parallelize(predictions)
    val ratingsAndPredictions = testData.map { case Rating(user, product, rating) =>
      ((user, product), rating)
    }.join(predictionsRDD)
    val mse = ratingsAndPredictions.map { case (_, (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    mse
  }

  // Main function to set up Spark Streaming and process the data
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(getClass.getClassLoader.getResource("log4j.properties"))
    logger.setLevel(Level.INFO)
    logger.info("Log4j is configured correctly.")
    val spark = SparkSession.builder().appName("Real-Time Recommendations").master("local[*]").getOrCreate()

    // Reading ratings data from a file
    val ratingsFilePath = "D:\\projectsparkFN\\logs\\ratings.txt"
    val ratingsRDD = spark.sparkContext.textFile(ratingsFilePath).map(parseInput)

    // Filtering out negative ratings
    val filteredRatingsRDD = ratingsRDD.filter { case (_, _, rating) => rating > 0.0 }

    // Configuring Kafka producer
    val kafkaProps = new java.util.Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProps.put("value.serializer", classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](kafkaProps)

    // Sending station data to Kafka
    stationData.foreach { case (stationId, stationName) =>
      val record = new ProducerRecord("station-data", stationId.toString, stationName)
      producer.send(record)
      logger.info(s"Message sent to Kafka: $stationId - $stationName")
    }

    // Setting up Spark Streaming context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Kafka consumer parameters
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[StringDeserializer].getName,
      "group.id" -> "groupID1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("station-data")
    logger.info("Starting Kafka Producer")

    // Creating a Kafka stream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    logger.info("Starting Kafka Consumer")
    val userID = 200

    // Processing each RDD in the stream
    stream.foreachRDD { rdd =>
      logger.info("Checking for new data in the stream.")
      if (!rdd.isEmpty()) {
        logger.info(s"RDD count: ${rdd.count()}")
        rdd.foreach { record =>
          logger.info(s"Key: ${record.key()}, Value: ${record.value()}")
        }

        // Processing station data from the stream
        val stationStream = rdd.map(record => (record.key.toInt, record.value.toFloat))
        stationStream.foreach { case (stationId, rate) =>
          logger.info(s"Station Stream - ID: $stationId, Rate: $rate")
        }

        // Creating ratings for ALS from the stream
        val ratingsForALS = stationStream.map { case (stationId, rate) =>
          Rating(userID, stationId, rate)
        }
        ratingsForALS.foreach { rating =>
          logger.info(s"Rating for ALS - User: ${rating.user}, Station: ${rating.product}, Rating: ${rating.rating}")
        }

        // Combining stream ratings with historical ratings
        val combinedRatingsRDD = ratingsForALS.union(filteredRatingsRDD.map { case (user, product, rating) =>
          Rating(user, product, rating)
        })

        combinedRatingsRDD.foreach { rating =>
          logger.info(s"Combined Rating - User: ${rating.user}, Station: ${rating.product}, Rating: ${rating.rating}")
        }

        // Model training and evaluation parameters
        val (bestRank, bestIterations, bestLambda) = {
          val ranks = List(5, 10, 15)
          val iterations = List(10, 20, 30)
          val lambdas = List(0.01, 0.1, 1.0)
          val allParams = for {
            rank <- ranks
            iter <- iterations
            lambda <- lambdas
          } yield (rank, iter, lambda)

          // Evaluating different model parameters to find the best model
          val evaluations = allParams.map { case (rank, iter, lambda) =>
            val model = ALS.train(combinedRatingsRDD, rank, iter, lambda)
            val recommendations = model.recommendProducts(userID, 5)
            val ratingsForEvaluation = rdd.map { record =>
              val fields = record.value.split("\\s")
              Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
            }
            val score = evaluateModel(recommendations, ratingsForEvaluation)
            logger.info(s"Evaluating model with rank=$rank, iterations=$iter, lambda=$lambda: MSE=$score")
            ((rank, iter, lambda), score)
          }
          val bestParams = evaluations.minBy(_._2)._1
          logger.info(s"Best Model Params - Rank: ${bestParams._1}, Iterations: ${bestParams._2}, Lambda: ${bestParams._3}")
          (bestParams._1, bestParams._2, bestParams._3)
        }
         println()
        // Training the best model
        val bestModel = ALS.train(combinedRatingsRDD, bestRank, bestIterations, bestLambda)
        logger.info(s"Best Model Params - Rank: $bestRank, Iterations: $bestIterations, Lambda: $bestLambda")

        // Generating recommendations
        val recommendations = bestModel.recommendProducts(userID, 5)
        logger.info(recommendations)
        displayRecommendations(userID, recommendations, stationData, producer)

        // Logging the generated recommendations
        logger.info("Generated recommendations:")
        recommendationsBuffer.foreach(println)
      } else {
        logger.info("No new data in the stream.")

      }
      logger.info("Generated recommendations:")
      recommendationsBuffer.foreach(println)

    }
    ssc.start()
    ssc.awaitTermination()
  }
}

