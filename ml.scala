
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame 
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType 
import org.apache.spark.sql.types.DoubleType 
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vectors
import sqlContext.implicits._
import org.apache.spark.ml.regression.DecisionTreeRegressor

/* First at all create HIVE CONTEXT, but it's not our case 'cos of spark-shell  */

/* Switch database   */
sqlContext.sql("use elena_prova")

val table_a = sqlContext.sql("select * from a_ml")
val table_b = sqlContext.sql("select * from b_ml")

 val joined = table_a.join(table_b, table_a("id") === table_b("id"), "inner").select("age","weight", "high")


/*******************************************************************************************/
trait ourParameters extends Params {
  val inputCol1 = new Param[String](this, "inputCol1", "input column 1")
  val inputCol2 = new Param[String](this, "inputCol2", "input column 2")
  val outputCol1 = new Param[String](this, "outputCol1", "output column 1")
  val outputCol2 = new Param[String](this, "outputCol2", "output column 2")

  def pvals(pm: ParamMap) = (
    // this code can be cleaner in versions of Spark after 1.3
    pm.get(inputCol1).getOrElse("age"),
    pm.get(inputCol2).getOrElse("high"),
    pm.get(outputCol1).getOrElse("age_years"),
    pm.get(outputCol2).getOrElse("high_meters")
  )
}

class ourTransfomers(override val uid: String) extends Transformer with ourParameters {

  def this() = this(Identifiable.randomUID("OURTRA"))
  var pm: ParamMap = ParamMap()

  def setParam(p : ParamMap) = 
  {
  	 pm = p
  }
  def transformSchema(schema: StructType) = {
    val col1 = pm.get(outputCol1).getOrElse("age_years")
    val col2 = pm.get(outputCol2).getOrElse("high_meters")
    StructType(Array(StructField("label",DoubleType,true), StructField("features",new org.apache.spark.mllib.linalg.VectorUDT, true)))
  }
 
  override
  def transform( dataset:DataFrame) = 
  {
  	transform(dataset, pm)
  }

  override 
  def transform(dataset: DataFrame, paramMap: ParamMap) = {
    val (inCol1, inCol2, outCol1, outCol2) = pvals(paramMap)
    val ds = dataset.withColumn(outCol1, dataset(inCol1) / 12.0 ).withColumn(outCol2, dataset(inCol2) / 100.0).select("weight", outCol1, outCol2).withColumnRenamed("weight", "label")
    ds.map( x=> (x(0).toString.toDouble, Vectors.dense(Array(x(1).toString.toDouble, x(2).toString.toDouble)))).toDF("label", "features")
  }
  def copy(extra: ParamMap ) : ourTransfomers = defaultCopy(extra)
}

/*******************************************************************************************/

val inputCol1 = new Param[String]("a", "inputCol1", "input column 1")
val inputCol2 = new Param[String]("b", "inputCol2", "input column 2")
val outputCol1 = new Param[String]("c", "outputCol1", "output column 1")
val outputCol2 = new Param[String]("d", "outputCol2", "output column 2")

val myP = ParamMap().put(outputCol1 -> "Age_YeArs", outputCol2 -> "High_MetErs", inputCol1 -> "age", inputCol2 -> "high")

val t1 = new ourTransfomers()
t1.setParam(myP)
val lr = new DecisionTreeRegressor().setMaxBins(1024).setMaxDepth(30).setImpurity("variance")

val pipeline = new Pipeline().setStages(Array(t1, lr))

val model = pipeline.fit(joined)

model.transform(joined)
