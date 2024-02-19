package fonctions

import org.apache.spark.sql.{DataFrame, SaveMode}


object read_write {

        def writeHiveMlutiSim(dataFrame: DataFrame, header: Boolean, compression: String, path: String, table: String) : Unit =  {

          dataFrame

              .write

              .mode(SaveMode.Ignore)

              .partitionBy("year", "month")

              .option("header", header)

              .option("compression", compression)

              .option("path", path)

              .saveAsTable(table)

        }
}
