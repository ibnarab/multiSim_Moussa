package fonctions

import org.apache.spark.sql.DataFrame
import constants._
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object utils {

          def dualSimDf(table: String, year: String, month: String): DataFrame = {

            val df = spark.sql(
              s"""
                 |SELECT
                 |   TRIM(caller_msisdn)         AS msisdn      ,
                 |   TRIM(equipment_identity)    AS imei        ,
                 |   TRIM(muuti_sim)             AS multi_sim   ,
                 |   $year                       AS year        ,
                 |   $month                      AS month
                 |FROM $table
               """.stripMargin
            )

            df

          }



          def traficVoixSmsDf(table: String, debut: String, fin: String): DataFrame = {

              spark.sql(
                s"""
                   |SELECT
                   |     TRIM(caller_msisdn)         AS msisdn             ,
                   |     TRIM(equipment_identity)    AS imei               ,
                   |     TRIM(op_called_msisdn)      AS op_called_msisdn   ,
                   |     TRIM(nombre)                AS nombre             ,
                   |     TRIM(dureeappel)            AS dureeappel
                   |FROM $table
                   |WHERE
                   |   day
                   |BETWEEN '$debut' AND '$fin'
                   |          AND
                   |   traffic_direction = "SORTANT"
                   |          AND
                   |   op_called_msisdn IN ('ORA', 'TIG', 'EXM')
                   |          AND
                   |   traffic_type = "VOIX"
                 """.stripMargin)

          }


          def topAppel(table: String, debut: String, fin: String): DataFrame = {

            val w1 = Window.partitionBy("msisdn").orderBy(desc("nombre"))

            val df = spark.sql(
              s"""
                 |SELECT
                 |     TRIM(caller_msisdn)            AS msisdn           ,
                 |     TRIM(corresp_msisdn)           AS corresp_msisdn   ,
                 |     SUM(nombre)                    AS nombre
                 |FROM $table
                 |WHERE
                 |   day
                 |BETWEEN '$debut' AND '$fin'
                 |                 AND
                 |   traffic_direction = "SORTANT"
                 |                 AND
                 |    traffic_type = "VOIX"
                 |GROUP BY
                 |    msisdn                                               ,
                 |    corresp_msisdn
                     """.stripMargin)

             val dfRank = df
               .withColumn("row",row_number.over(w1)).where(col("row") <= 5).drop("row")
               .withColumn("corresp_msisdn",
                 when(length(col("corresp_msisdn")) === 12,
                   when(col("corresp_msisdn").startsWith("191") || col("corresp_msisdn").startsWith("192") || col("corresp_msisdn").startsWith("193"),
                     substring(col("corresp_msisdn"), 4, 9)
                   ).otherwise(col("corresp_msisdn"))
                 ).otherwise(col("corresp_msisdn"))
               )

            val dfRank2 = dfRank.groupBy("msisdn").agg(collect_list("corresp_msisdn").alias("top_appel"))

            // Convertir la liste en chaîne de caractères avec un délimiteur ","
            val dfWithStringColumn = dfRank2.withColumn("top_appel", col("top_appel").cast("string"))

            dfWithStringColumn

          }





          def dailyClients(table: String, fin: String): DataFrame = {

            spark.sql(
              s"""
                 |SELECT
                 |    TRIM(msisdn)            AS msisdn           ,
                 |    TRIM(age)               AS age              ,
                 |    TRIM(sex)               AS sex              ,
                 |    TRIM(segment_marche)    AS segment_marche
                 |FROM $table
                 |WHERE
                 |    day = '$fin'
                 """.stripMargin)

          }



          def locationDaytimeDf(table: String, debut: String, fin: String): DataFrame = {

            val w1 = Window.partitionBy("msisdn").orderBy(desc("count"))

            val df = spark.sql(
              s"""
                 |SELECT
                 |   TRIM(msisdn)         AS  msisdn                    ,
                 |   TRIM(ca_cr_commune)  AS  ca_cr_commune_day         ,
                 |   COUNT(*)             AS  count
                 |FROM $table
                 |WHERE day BETWEEN '$debut' AND '$fin'
                 |GROUP BY
                 |    msisdn              ,
                 |    ca_cr_commune_day
                         """.stripMargin)
            df.withColumn("row1",row_number.over(w1)).where(col("row1") === 1).drop("row1")
          }



          def locationNighttimeDf(table: String, debut: String, fin: String): DataFrame = {

            val w1 = Window.partitionBy("msisdn").orderBy(desc("count"))

            val df = spark.sql(
              s"""
                 |SELECT
                 |   TRIM(msisdn)         AS msisdn                 ,
                 |   TRIM(ca_cr_commune)  AS ca_cr_commune_night    ,
                 |   COUNT(*)             AS count
                 |FROM $table
                 |WHERE day BETWEEN '$debut' AND '$fin'
                 |GROUP BY
                 |    msisdn                                        ,
                 |    ca_cr_commune_night
                             """.stripMargin)

            df.withColumn("row1",row_number.over(w1)).where(col("row1") === 1).drop("row1")
          }



          def usageDataDf(table: String, debut: String, fin: String): DataFrame = {
            spark.sql(
              s"""
                 |SELECT
                 |     TRIM(msisdn)                  AS msisdn            ,
                 |     SUM(data_volume)              AS  usage_data_90j
                 |FROM $table
                 |WHERE day BETWEEN '$debut' AND '$fin'
                 |GROUP BY
                 |      msisdn
                             """.stripMargin)
          }




          def tableFinalDf1(dualSimDf: DataFrame, traficVoixSmsDf: DataFrame, dailyClients: DataFrame, locationDaytimeDf: DataFrame, locationNighttimeDf: DataFrame, usageDataDf: DataFrame, topAppel: DataFrame): DataFrame = {


              dualSimDf
                .join(traficVoixSmsDf       , Seq("msisdn")   , "left")
                .join(dailyClients          , Seq("msisdn")   , "left")
                .join(locationDaytimeDf     , Seq("msisdn")   , "left")
                .join(locationNighttimeDf   , Seq("msisdn")   , "left")
                .join(usageDataDf           , Seq("msisdn")   , "left")
                .join(topAppel              , Seq("msisdn")   , "left")
                .groupBy(
                  dualSimDf("msisdn")             , dualSimDf("imei")                     , dualSimDf("multi_sim"), dailyClients("age"), dailyClients("sex"),
                  dailyClients("segment_marche")  , locationDaytimeDf("ca_cr_commune_day"), locationNighttimeDf("ca_cr_commune_night") ,
                  usageDataDf("usage_data_90j")   , topAppel("top_appel")                 , dualSimDf("year"), dualSimDf("month"))
                .agg(
                  sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "ORA"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_orange_3_last_m")        ,
                  sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "TIG"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_free_3_last_m")          ,
                  sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "EXM"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_expresso_3_last_m")      ,
                  sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "ORA"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_orange_3_last_m")   ,
                  sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "TIG"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_free_3_last_m")     ,
                  sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "EXM"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_expresso_3_last_m")
                )
                .select(
                  dualSimDf           ("msisdn"                                       )       ,
                  dualSimDf           ("imei"                                         )       ,
                  dualSimDf           ("multi_sim"                                    )       ,
                  dailyClients        ("age"                                          )       ,
                  dailyClients        ("sex"                                          )       ,
                  dailyClients        ("segment_marche"                               )       ,
                  col                 ("appel_orange_3_last_m"              )       ,
                  col                 ("appel_free_3_last_m"                )       ,
                  col                 ("appel_expresso_3_last_m"            )       ,
                  col                 ("min_appel_orange_3_last_m"          )       ,
                  col                 ("min_appel_free_3_last_m"            )       ,
                  col                 ("min_appel_expresso_3_last_m"        )       ,
                  locationDaytimeDf   ("ca_cr_commune_day"                            )       ,
                  locationNighttimeDf ("ca_cr_commune_night"                          )       ,
                  usageDataDf         ("usage_data_90j"                               )       ,
                  topAppel            ("top_appel"                                    )       ,
                  dualSimDf           ("year"                                         )       ,
                  dualSimDf           ("month"                                        )
                )
                .na.fill(Map("usage_data_90j" -> 0))
                .coalesce(100)
            }


      def tableFinalDf2(traficVoixSmsDf: DataFrame, dailyClients: DataFrame, locationDaytimeDf: DataFrame, locationNighttimeDf: DataFrame, usageDataDf: DataFrame, topAppel: DataFrame): DataFrame = {


        val df = traficVoixSmsDf
          .join(dailyClients          , Seq("msisdn")   , "left")
          .join(locationDaytimeDf     , Seq("msisdn")   , "left")
          .join(locationNighttimeDf   , Seq("msisdn")   , "left")
          .join(usageDataDf           , Seq("msisdn")   , "left")
          .join(topAppel              , Seq("msisdn")   , "left")
          .groupBy(
            //dualSimDf("msisdn")             , dualSimDf("imei")                     , dualSimDf("multi_sim"),
            traficVoixSmsDf("msisdn"), dailyClients("age"), dailyClients("sex"), dailyClients("segment_marche")  , locationDaytimeDf("ca_cr_commune_day"),
            locationNighttimeDf("ca_cr_commune_night") , usageDataDf("usage_data_90j")   , topAppel("top_appel")
            //dualSimDf("year"), dualSimDf("month")
          )
          .agg(
            sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "ORA"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_orange_3_last_m")        ,
            sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "TIG"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_free_3_last_m")          ,
            sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "EXM"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_expresso_3_last_m")      ,
            sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "ORA"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_orange_3_last_m")   ,
            sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "TIG"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_free_3_last_m")     ,
            sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "EXM"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_expresso_3_last_m")
          )
          .select(
            /*dualSimDf           ("msisdn"                                       )       ,
            dualSimDf           ("imei"                                         )       ,
            dualSimDf           ("multi_sim"                                    )       ,*/
            traficVoixSmsDf     ("msisdn"                                       )       ,
            dailyClients        ("age"                                          )       ,
            dailyClients        ("sex"                                          )       ,
            dailyClients        ("segment_marche"                               )       ,
            col                 ("appel_orange_3_last_m"              )       ,
            col                 ("appel_free_3_last_m"                )       ,
            col                 ("appel_expresso_3_last_m"            )       ,
            col                 ("min_appel_orange_3_last_m"          )       ,
            col                 ("min_appel_free_3_last_m"            )       ,
            col                 ("min_appel_expresso_3_last_m"        )       ,
            locationDaytimeDf   ("ca_cr_commune_day"                            )       ,
            locationNighttimeDf ("ca_cr_commune_night"                          )       ,
            usageDataDf         ("usage_data_90j"                               )       ,
            topAppel            ("top_appel"                                    )
            /*dualSimDf           ("year"                                         )       ,
            dualSimDf           ("month"                                        )*/
          )
          .na.fill(Map("usage_data_90j" -> 0))
          .coalesce(100)

        val dfUnique = df.withColumn("numero", monotonically_increasing_id() + 1).drop("msisdn")

        dfUnique.select("numero", "age", "sex", "segment_marche", "appel_orange_3_last_m", "appel_free_3_last_m", "appel_expresso_3_last_m", "min_appel_orange_3_last_m",
        "min_appel_free_3_last_m", "min_appel_expresso_3_last_m", "ca_cr_commune_day", "ca_cr_commune_night", "usage_data_90j", "top_appel")

      }


}
