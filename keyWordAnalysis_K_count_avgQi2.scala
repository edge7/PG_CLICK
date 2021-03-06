/** Keyword Cluster Analysis  ******************************************************/
/**    covariates: keyword count, quota_impression average linked to each keyword  */
/**    method:     K-Means                                                         */
/***********************************************************************************/

/* libraries */
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors


/* Input data *******************************************************************************************************************************************************/
// column 5 defines the keyword state: we only consider the "enabled" labeled keywords
// keywords are stored in column 3: we impose the lower case format and remove the gaps
// quota_impression covariate is stored in column 15
val rawData = sc.textFile("hdfs://172.31.6.53/user/hdfs/pg_click/*")


/* Data processing */

// each keyword is liked to its count:
// keywordEnabledCount --> Tuple(keyword, count)
val keywordEnabled = rawData.filter( x=> x.split(";")(5).equals("enabled") ).map( x => x.split(";")(3).toLowerCase.replace(" ", "") )
keywordEnabled.cache
val keywordEnabledCount = keywordEnabled.countByValue()
keywordEnabled.unpersist()
// Print keyword most frequently used
// keywordEnabledCount.toSeq.sortBy( _._2 ).reverse.foreach(println)

// each keyword is liked to its quota_impression average:
// keywordEnabledMean --> Tuple(keyword, qi_avg)
val keywordEnabled_QI = rawData.filter( x=> x.split(";")(5).equals("enabled") ).map( x => ( x.split(";")(3).toLowerCase.replace(" ", ""), x.split(";")(15).toDouble ) )
keywordEnabled_QI.cache
val keywordEnabled_QI_groupBy = keywordEnabled_QI.groupBy(_._1)
val table = keywordEnabled_QI_groupBy.map( x=> ( x._1, x._2.map( y => y._2) ) ) // cleaned keywordEnabled_QI_groupBy
val keywordEnabledMean = table.map( x => ( x._1, x._2.reduce(_+_) / x._2.size ) ).collect.toMap

// each keyword is liked to its count and its quota_impression average:
// mixed --> Tuple(keyword, count, qi_avg)
val mixed = keywordEnabledCount.toSeq.sortBy(_._1).zip( keywordEnabledMean.toSeq.sortBy(_._1))

// mixed values are saved in order to clustering
val key_count_qi = mixed.map( x => Array(x._1._2, x._2._2) )
val dataToTrain = key_count_qi.map( x => Vectors.dense(x) )   // 2D covariate vector type that is required by the KMeans.train method
val dataToTrainRDD = sc.parallelize(dataToTrain).cache


/* Keyword Clustering via K-Means ************************************************************************************************************************************/

// parameters
val numIterations = 50

// model is trained with different number of required cluster K:
// "listDiff" contains the trained model "model" and a model goodness measure "diffFromCentr" for each K
val start = System.currentTimeMillis; // tic
val listDiff = 
for( i <- ( 1 to 100  by 5) )
yield
{ 
     val model = KMeans.train(dataToTrainRDD, i, numIterations)
     val info = dataToTrainRDD.map( x => (x, model.predict(x), model.clusterCenters( model.predict(x) )  ))
     val diffFromCentr = info.map ( x => Math.abs(x._1.apply(0) - x._3.apply(0)) ).mean()
     (i, diffFromCentr, model)
}
val end = System.currentTimeMillis;  // toc

// once we get the list, we sort it by the goodness measure: the chosen model is linked to K=36
// let us suppose it is called modelToSave
sc.parallelize(Seq(modelToSave), 1).saveAsObjectFile("hdfs://172.31.6.53/user/root/kMeans36New.model")



// ???????  TUTTO IL CODICE SEGUENTE è ELIMINABILE VERO?  ???????
 import java.io.FileOutputStream
 import java.io.ObjectOutputStream
 val fos = new FileOutputStream("clusterModelKeyWordCount")
 val oos = new ObjectOutputStream(fos)  
 oos.writeObject(modelToSave)  
 oos.close

 import java.io.FileInputStream
 import java.io.ObjectInputStream
 val fos = new FileInputStream("clusterModelKeyWordCount")
 val oos = new ObjectInputStream(fos)
 val newModel = oos.readObject().asInstanceOf[org.apache.spark.mllib.clustering.KMeansModel]
