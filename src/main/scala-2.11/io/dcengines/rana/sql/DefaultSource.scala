package io.dcengines.rana.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

/**
  * Created by rana on 26/9/16.
  */
class DefaultSource extends RelationProvider with DataSourceRegister with SchemaRelationProvider {
  override def shortName(): String = "parquet-test"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) => new ParquetTestRelation(sqlContext, p, schema)
      case _ => throw new IllegalArgumentException("Path is reauired for parquet-test format!!")
    }
  }
}