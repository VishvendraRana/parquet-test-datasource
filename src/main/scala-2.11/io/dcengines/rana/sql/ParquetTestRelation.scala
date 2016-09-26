package io.dcengines.rana.sql

import org.apache.spark.sql.{SQLContext, types}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._

/**
  * Created by rana on 26/9/16.
  */
class ParquetTestRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType)
  extends BaseRelation with Serializable {
  override def schema: StructType = {
    if (this.userSchema != null) {
      this.userSchema
    } else {
      StructType(
        StructField("id", LongType, false) ::
        StructField("name", StringType, true) ::
        StructField("gender", BooleanType, true) :: Nil
      )
    }
  }
}
