package AlbumAnalysis

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}  // 用于 Table 接口

object Albums_analysis {
  def main(args: Array[String]): Unit = {
    // TODO: 创建spark环境
    val spark = SparkSession.builder()
      .appName("Albums_analysis")
      .master("yarn")
      .getOrCreate()

    // TODO: 0.数据预处理

    //读取数据
    var df = spark.read
      .option("header", true)
      .csv("datas/albums.csv")
    // 处理缺失值
    df = df.na.drop(Seq("id", "year_of_pub", "num_of_sales"))

    // TODO: 1. 统计各类型专辑的数量
    val genreCount = df.groupBy("genre")
      .count()
      .orderBy(desc("count"))
    println("各类型专辑数量统计:")
    genreCount.show()

    // TODO: 2. 统计各类型专辑的销量总数
    val genreSales = df.groupBy("genre")
      .agg(sum("num_of_sales").alias("total_sales"))
      .orderBy(desc("total_sales"))
    println("\n各类型专辑总销量统计:")
    genreSales.show()

    // TODO: 3. 统计近20年每年发行的专辑数量和单曲数量
    val yearlyStats = df
      .groupBy("year_of_pub")
      .agg(
        count("id").alias("album_count"),
        sum("num_of_tracks").alias("total_tracks")
      )
      .orderBy("year_of_pub")
    println(s"\n近20年年度统计:")
    yearlyStats.show(20)

    // TODO: 4. 分析总销量前五的专辑类型的各年份销量
    val top5Genres = genreSales.select("genre").limit(5).collect().map(_.getString(0))

    val yearlyGenreSales = df.filter(col("genre").isin(top5Genres: _*))
      .groupBy("year_of_pub", "genre")
      .agg(sum("num_of_sales").alias("yearly_sales"))
      .orderBy(desc("yearly_sales"))

    val pivotSales = yearlyGenreSales
      .groupBy("year_of_pub")
      .pivot("genre", top5Genres)
      .agg(first("yearly_sales"))
      .orderBy("year_of_pub")

    println("\n前五类型各年份销量统计:")
    pivotSales.show()

    // TODO: 5. 分析总销量前五的专辑类型在不同评分体系中的平均评分
    val avgRatings = df.filter(col("genre").isin(top5Genres: _*))
      .groupBy("genre")
      .agg(
        avg("rolling_stone_critic").alias("avg_rolling_stone"),
        avg("mtv_critic").alias("avg_mtv"),
        avg("music_maniac_critic").alias("avg_music_maniac")
      )
      .orderBy("genre")
    println("\n前五类型平均评分统计:")
    avgRatings.show()

    // TODO: 写入hbase
    writeToHBase(genreCount, "genre_count", "genre")
    writeToHBase(genreSales, "genre_sales", "genre")
    writeToHBase(yearlyStats, "yearly_stats", "year_of_pub")
    writeToHBase(pivotSales, "top5_yearly_sales", "year_of_pub")
    writeToHBase(avgRatings, "top5_ratings", "genre")
    println("\n已将结果写入hbase")
    // TODO: 释放spark环境
    spark.stop()

  }

  //HBase写入方法
  private def writeToHBase(df: DataFrame, tableName: String, rowKeyCol: String): Unit = {
    df.rdd.foreachPartition { iter =>
      // 在闭包内部创建HBase配置和连接
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.addResource(new Path("file:///opt/module/hbase/conf/hbase-site.xml")) // 显式加载配置文件
      hbaseConf.set("hbase.zookeeper.quorum", "master,worker1,worker2")

      var connection: Connection = null
      var table: Table = null

      try {
        connection = ConnectionFactory.createConnection(hbaseConf)
        table = connection.getTable(TableName.valueOf(tableName))

        iter.foreach { row =>
          // 添加空值保护
          val rowKey = row.getAs[Any](rowKeyCol) match {
            case null => throw new IllegalArgumentException(s"Row key column $rowKeyCol contains null")
            case value => Bytes.toBytes(value.toString)
          }

          val put = new Put(rowKey)
          row.schema.fieldNames
            .filter(_ != rowKeyCol)
            .foreach { colName =>
              val value = row.getAs[Any](colName) match {
                case null => Bytes.toBytes("") // 处理空值
                case v => Bytes.toBytes(v.toString)
              }
              put.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes(colName),
                value
              )
            }
          table.put(put)
        }
      } finally {
        if (table != null) table.close()
        if (connection != null) connection.close()
      }
    }
  }
}
