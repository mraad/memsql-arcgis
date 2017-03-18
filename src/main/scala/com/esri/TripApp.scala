package com.esri

import com.esri.hex.HexGrid
import com.esri.mercator._
import com.memsql.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.ParseModes
import org.apache.spark.sql.types.StructType

object TripApp extends App {
  val spark = SparkSession
    .builder()
    .appName("ETL NYC Trips To MemSQL")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  try {
    import spark.implicits._

    val conf = spark.sparkContext.getConf
      .set("spark.memsql.host", "localhost")
      .set("spark.memsql.user", "root")
      .set("spark.memsql.password", "")

    val xmin = conf.getDouble("spark.app.xmin", -75.0)
    val ymin = conf.getDouble("spark.app.ymin", 40.0)
    val xmax = conf.getDouble("spark.app.xmax", -72.0)
    val ymax = conf.getDouble("spark.app.ymax", 41.0)

    val hc = spark.sparkContext.hadoopConfiguration
    hc.set("fs.s3a.access.key", conf.get("spark.app.access.key"))
    hc.set("fs.s3a.secret.key", conf.get("spark.app.secret.key"))

    val schema = ScalaReflection.schemaFor[TripInp].dataType.asInstanceOf[StructType]
    spark
      .read
      .format("csv")
      .option("delimiter", conf.get("spark.app.input.delimiter", ","))
      .option("header", conf.getBoolean("spark.app.input.header", true))
      .option("mode", ParseModes.DROP_MALFORMED_MODE)
      .option("timestampFormat", conf.get("spark.app.input.timestampFormat", "yyyy-MM-dd HH:mm:ss"))
      .schema(schema)
      .load(conf.get("spark.app.input.path", "s3a://mraad-taxis/trips-1M.csv"))
      .as[TripInp]
      .filter(trip => {
        xmin < trip.plon && trip.plon < xmax &&
          ymin < trip.plat && trip.plat < ymax &&
          xmin < trip.dlon && trip.dlon < xmax &&
          ymin < trip.dlat && trip.dlat < ymax
      })
      .mapPartitions(iter => {
        val hex100 = new HexGrid(100, -20000000.0, -20000000.0)
        iter.map(trip => {
          val px = trip.plon toMercatorX
          val py = trip.plat toMercatorY
          val dx = trip.dlon toMercatorX
          val dy = trip.dlat toMercatorY
          val p100 = hex100.convertXYToRowCol(px, py).toText
          val d100 = hex100.convertXYToRowCol(dx, dy).toText
          val pPoint = f"POINT(${trip.plon}%.6f ${trip.plat}%.6f)"
          val dPoint = f"POINT(${trip.dlon}%.6f ${trip.dlat}%.6f)"
          TripOut(trip.pdate, trip.ddate, trip.passcount, trip.triptime, trip.tripdist, pPoint, dPoint, px, py, dx, dy, p100, d100)
        })
      })
      .toDF()
      .saveToMemSQL(
        conf.get("spark.app.database", "trips"),
        conf.get("spark.app.table", "trips")
      )

  } finally {
    spark.stop()
  }

}
