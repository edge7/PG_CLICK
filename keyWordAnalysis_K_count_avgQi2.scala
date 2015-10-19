
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

val rawData = sc.textFile("hdfs://172.31.6.53/user/hdfs/pg_click/*")
val keywordEnabled = rawData.filter( x=> x.split(";")(5).equals("enabled") ).map( x => x.split(";")(3).toLowerCase.replace(" ", "") )
keywordEnabled.cache 

val keywordEnabledCount = keywordEnabled.countByValue()


val justCount = keywordEnabledCount.values.toArray
val keywordEnabled_QI = rawData.filter( x=> x.split(";")(5).equals("enabled") ).map( x => ( x.split(";")(3).toLowerCase.replace(" ", ""), x.split(";")(15).toDouble ) )
keywordEnabled_QI.cache 

val keywordEnabled_QI_groupBy = keywordEnabled_QI.groupBy(_._1)
val table = keywordEnabled_QI_groupBy.map( x=> (x._1, x._2.map( y => y._2) ))

/* keywordEnabledK --> Tuple( keyword, mean ) */
val keywordEnabledMean = table.map( x => ( x._1, x._2.reduce(_+_) / x._2.size ) ).collect.toMap

val mixed = keywordEnabledCount.toSeq.sortBy(_._1).zip( keywordEnabledMean.toSeq.sortBy(_._1))
val key_count_qi = mixed.map( x=>  Array(x._1._2, x._2._2)) 
//val meanCount = justCount.reduce( (x,y) => x + y ).toDouble / justCount.size 
//val sdCount   = Math.sqrt( ( justCount.map( x => Math.pow((x - meanCount), 2) ).reduce( _+_ ) ) / (justCount.size -1).toDouble ) 
//val justCountStd = justCount.map( x => ( x - meanCount ) / sdCount )
val dataToTrain = key_count_qi.map( x => Vectors.dense(x) )
keywordEnabled.unpersist()

val dataToTrainRDD = sc.parallelize(dataToTrain).cache

/* Print keyword most frequently used */
// keywordEnabledCount.toSeq.sortBy( _._2 ).reverse.foreach(println)

// Cluster the data into two classes using KMeans
val numIterations = 50
val start = System.currentTimeMillis;
val listDiff = 
for( i <- ( 1 to 100  by 5) )
yield
{ 
     val model = KMeans.train(dataToTrainRDD, i, numIterations)
     val info = dataToTrainRDD.map( x => (x, model.predict(x), model.clusterCenters( model.predict(x) )  ))
     val diffFromCentr = info.map ( x => Math.abs(x._1.apply(0) - x._3.apply(0)) ).mean()
     (i, diffFromCentr, model)
}
val end = System.currentTimeMillis;

/* 21 K, create and save model */
val listDiffM = 
for( i <- Array(21, 21, 21, 21, 21, 21, 21) )
yield
{ 
     val model = KMeans.train(dataToTrainRDD, i, numIterations)
     val info = dataToTrainRDD.map( x => (x, model.predict(x), model.clusterCenters( model.predict(x) )  ))
     val diffFromCentr = info.map ( x => Math.abs(x._1.apply(0) - x._3.apply(0)) ).mean()
     (i, diffFromCentr, model)
}
/*Here we get list sorted and then get the first model */


sc.parallelize(Seq(modelToSave), 1).saveAsObjectFile("hdfs://172.31.6.53/user/root/kMeans36New.model")

/*Let's Suppose it is called modelToSave */
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
