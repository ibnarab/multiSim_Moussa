package fonctions

import org.apache.spark.sql.{DataFrame, SaveMode}


object read_write {

        def writeHiveMlutiSim(dataFrame: DataFrame, header: Boolean, compression: String, path: String, table: String) : Unit =  {

          dataFrame

              .repartition(1)

              .write

              .mode(SaveMode.Ignore)

              .partitionBy("year", "month")

              .option("header", header)

              .option("compression", compression)

              .option("path", path)

              .saveAsTable(table)

        }



      def writeInMysql(dataFrame: DataFrame, mode: String, table: String): Unit = {

        dataFrame
          .write
          .format("jdbc")
          .mode(mode)
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("url", "jdbc:mysql://10.137.15.6:3306/MYSVENTEREBONDPRD")
          .option("dbtable", table)
          .option("user", "u_vente_rebond")
          .option("password", "S0n@telV3nteReb0nd2023")
          //.option("batchsize", jdbcBatchSize)
          .save()
      }
}
