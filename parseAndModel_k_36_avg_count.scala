import java.io.FileInputStream
import java.io.ObjectInputStream
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils




val kModel = sc.objectFile[org.apache.spark.mllib.clustering.KMeansModel]("hdfs://172.31.6.53/user/root/kMeans36New.model").first()


/* Loading data from HDFS */
val rawData = sc.textFile("hdfs://172.31.6.53/user/hdfs/pg_click/*")
val rawDataGeneral = sc.textFile("hdfs://172.31.6.53/user/hdfs/history/*").filter( x=> ! x.split(",")(0).equals("naccount62229") )
//.map( x => (x ,x.split(",").size )  );

/*  KeyWordEnabled --> keywords with enabled state   */
val keywordEnabled = rawData.filter( x=> x.split(";")(5).equals("enabled") ).map( x => x.split(";")(3).toLowerCase.replace(" ", "") )
keywordEnabled.cache 

/*  keywordEnabledCount --> Map( keyword --> count, ... ) */
val keywordEnabledCount = keywordEnabled.countByValue()

/* keywordEnabledK --> Tuple( keyword, K_Label ) */
val keywordEnabledK = keywordEnabledCount.map ( x => (x._1, kModel.predict(Vectors.dense(x._2) ) ))

/* check #K with 0 count  */
val defaultK = kModel.predict(Vectors.dense(0, 0))  

/* We get Account, id_cliente, Keyword ItSelf, K of the KeyWord */
val keyWordTable = rawData.filter(x => x.split(";")(5).equals("enabled") ).map( s => ( (s.split(";")(0) + "_" + s.split(";")(1)), ( s.split(";")(3), keywordEnabledK.getOrElse(s.split(";")(3).replace(" ", ""), defaultK) ))).groupBy(_._1)

val keyWordTableMod = keyWordTable.map( x=> (x._1, x._2.map( y => y._2._2 ))).map( x=> ((x._1),( x._2.count( y => y == 0), x._2.count( y => y == 1), x._2.count( y => y == 2), x._2.count( y => y == 3), x._2.count( y => y == 4), x._2.count( y => y == 5),
x._2.count( y => y == 6), x._2.count( y => y == 7), x._2.count( y => y == 8),x._2.count( y => y == 9), x._2.count( y => y == 10), x._2.count( y => y == 11),
x._2.count( y => y == 12), x._2.count( y => y == 13), x._2.count( y => y == 14),x._2.count( y => y == 15), x._2.count( y => y == 16), x._2.count( y => y == 17),
x._2.count( y => y == 18), x._2.count( y => y == 19), x._2.count( y => y == 20)  )))  
/* We just get Account --> 0, idcliente  --> 1, impression --> 3, quotaImpr --> 8, maxCpc --> 12 */
val generalTable = rawDataGeneral.map( x => ( (x.split(",")(1).replace("-", "") + "_" + x.split(",")(0) ), ( x.split(",")(8).toDouble, x.split(",")(3).toDouble, x.split(",")(12).toDouble) ))

val tableJoint = generalTable.join(keyWordTableMod)

val dataReadyForModel = tableJoint.map( x=> Array.concat(x._2._1.productIterator.toArray, x._2._2.productIterator.toArray)).map( _.map(_.toString.toDouble))

/********** Start Training Model   **********/

val dataToTrain = dataReadyForModel.map( x => LabeledPoint( x(0), Vectors.dense(x.tail.map( x=> x) )) )


/******************************* LINEAR REGRESSION ***********************************************/


// Building the model
val numIterations = 300
val stepSize = 5*math.pow(10, -15);
val model = LinearRegressionWithSGD.train(dataToTrain, numIterations, stepSize)

// Evaluate model on training examples and compute training error
val valuesAndPreds = dataToTrain.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}

val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
println("training Mean Squared Error = " + MSE)

/****************************** RANDOM FOREST **************************************************/

val splits = dataToTrain.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))
trainingData.cache
testData.cache 
// Train a RandomForest model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.

val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 3 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "variance"
val maxDepth = 4
val maxBins = 1024

val testMSE = 
for ( numTrees <- (20 to 200 by 70);
	  maxDepth <- (4  to 20  by 5)   )
yield {
	
	val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
  											numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

	// Evaluate model on test instances and compute test error
	val labelsAndPredictions = testData.map { point =>
  									  		  val prediction = model.predict(point.features)
  										  		  (point.label, prediction)
											}
	val res = (numTrees, maxDepth, labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2) }.mean() )	
	scala.tools.nsc.io.File("res_Keyword").appendAll(res.toString + "\n");
	res

}

/* val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2) }.mean()
println("Test Mean Squared Error = " + testMSE)
println("Learned regression forest model:\n" + model.toDebugString) */

/***************************** BEST RESULT SO FAR 100, 14, 94.86 *******************************************/
var index = 0;
val resModel = 
for ( numTrees <- Array(100);
	  maxDepth <- Array(14, 14, 14)   )
yield {
	
	val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
  											numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

	// Evaluate model on test instances and compute test error
	val labelsAndPredictions = testData.map { point =>
  									  		  val prediction = model.predict(point.features)
  										  		  (point.label, prediction)
											}
     scala.tools.nsc.io.File("LOG_MODEL_TO_DELETE").appendAll(index + "\n");
     index = index +1;
	val res = (model, labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2) }.mean() )	
	res

}




/* Let 'model' be model choosen variable name */

/* Compute residuals */
val residuals = testData.map { point => 
							   val resid = point.label - model.predict(point.features)
							   (resid, point.label, model.predict(point.features))  
						      }
