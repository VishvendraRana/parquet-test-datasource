package io.dcengines.rana.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, types}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._

import scala.util.Try

/**
  * Created by rana on 26/9/16.
  */
class ParquetTestRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType)
  extends BaseRelation with Serializable with TableScan {

  override def schema: StructType = {
    if (this.userSchema != null) {
      this.userSchema
    } else {
      StructType(
        StructField("id", LongType, false) ::
        StructField("name", StringType, true) ::
        StructField("gender", StringType, true) :: Nil
      )
    }
  }

  override def buildScan(): RDD[Row] = {
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(x => x._2)
    val rows = rdd.map(file => {
      val lines = file.split("\n")
      val data = lines.map(line => line.split(",")).map(l => Seq(l(0).toLong, l(1),
        if(Try(l(2).trim.toInt == 1).getOrElse(true)) "Male" else "Female")).toSeq
      data.map(s => Row.fromSeq(s))
    })
    rows.flatMap(s => s)
  }
}
