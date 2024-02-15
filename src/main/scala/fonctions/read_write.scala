package fonctions

import fonctions.constants._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode}

object read_write {

        def writeHiveMlutiSim(dataFrame: DataFrame, header: Boolean, chemin: String, table: String) : Unit =  {
          dataFrame
            //.repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            //.partitionBy("year", "month", "day") // Spécifier les colonnes de partition
            .option("header", header)
            .option("path", chemin)
            .saveAsTable(table)
        }
}
