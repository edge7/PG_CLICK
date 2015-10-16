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


/* Loading data from HDFS */
val rawData = sc.textFile("/user/hdfs/pg_click/*")
val rawDataGeneral = sc.textFile("/user/hdfs/history/*").filter( x=> ! x.split(",")(0).equals("naccount62229") )
//.map( x => (x ,x.split(",").size )  );

/*  KeyWordEnabled --> keywords with enabled state   */
val keywordEnabled_QI = rawData.filter( x=> x.split(";")(5).equals("enabled") ).map( x => ( x.split(";")(3).toLowerCase.replace(" ", ""), x.split(";")(15).toDouble ) )
keywordEnabled_QI.cache 

val keywordEnabled_QI_groupBy = keywordEnabled_QI.groupBy(_._1)
val table = keywordEnabled_QI_groupBy.map( x=> (x._1, x._2.map( y => y._2) ))

/* keywordEnabledK --> Tuple( keyword, mean ) */
val keywordEnabledMean = table.map( x => ( x._1, x._2.reduce(_+_) / x._2.size ) ).collect.toMap

/* We get Account, id_cliente, Keyword ItSelf, mean of the KeyWord */
val defaultMean = 0; /* TODO */
val keyWordTable = rawData.filter(x => x.split(";")(5).equals("enabled") ).map( s => ( (s.split(";")(0) + "_" + s.split(";")(1)), ( s.split(";")(3), keywordEnabledMean.getOrElse(s.split(";")(3).replace(" ", ""), defaultMean) ))).groupBy(_._1)
val keyWordTableMod = keyWordTable.map( x=> (x._1, x._2.map( y => y._2 ))) 

/* We just get Account --> 0, idcliente  --> 1, impression --> 3, quotaImpr --> 8, maxCpc --> 12 */
val generalTable = rawDataGeneral.map( x => ( (x.split(",")(1).replace("-", "") + "_" + x.split(",")(0) ), ( x.split(",")(8).toDouble, x.split(",")(3).toDouble, x.split(",")(12).toDouble) ))

val tableJoint = generalTable.join(keyWordTableMod)

val dataReadyForModel = tableJoint.map( x=> Array.concat(x._2._1.productIterator.toArray, x._2._2.productIterator.toArray)).map( _.map(_.toString.toDouble))

/********** Start Training Model   **********/

val dataToTrain = dataReadyForModel.map( x => LabeledPoint( x(0), Vectors.dense(x.tail.map( x=> x) )) ).cache 