package fonctions

import org.apache.spark.sql.DataFrame
import constants._
import org.apache.spark.sql.functions._
import java.time.LocalDate
import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions.row_number


object utils {

      def dualSimDf(table: String): DataFrame = {
        spark.sql(
          s"""
             |SELECT
             |   TRIM(caller_msisdn)                AS msisdn          ,
             |   TRIM(equipment_identity)           AS imei            ,
             |   TRIM(muuti_sim)                    AS multi_sim
             |FROM $table
           """.stripMargin
        )
      }





      def traficVoixSmsDf(table: String, debut: String, fin: String): DataFrame = {
          spark.sql(
            s"""
               |SELECT
               |     TRIM(caller_msisdn)                                                                         AS msisdn                          ,
               |     TRIM(equipment_identity)                                                                    AS imei                            ,
               |     TRIM(op_called_msisdn)                                                                      AS op_called_msisdn                ,
               |     TRIM(nombre)                                                                                AS nombre                          ,
               |     TRIM(dureeappel)                                                                            AS dureeappel
               |FROM $table
               |WHERE
               |   day BETWEEN '$debut' AND '$fin'
               |          AND
               |   traffic_direction = "SORTANT" AND op_called_msisdn IN ('ORA', 'TIG', 'EXM') AND traffic_type = "VOIX"
             """.stripMargin)
      }

      def topAppel(table: String, debut: String, fin: String): DataFrame = {

        val w1 = Window.partitionBy("msisdn").orderBy(desc("nombre"))
        val df = spark.sql(
          s"""
             |SELECT
             |     TRIM(caller_msisdn)                                                                         AS msisdn                          ,
             |     TRIM(corresp_msisdn)                                                                        AS corresp_msisdn                  ,
             |     SUM(nombre)                                                                                 AS nombre
             |FROM $table
             |WHERE
             |   day BETWEEN '$debut' AND '$fin'
             |          AND
             |   traffic_direction = "SORTANT"  AND traffic_type = "VOIX"
             |GROUP BY msisdn, corresp_msisdn
                 """.stripMargin)

         val dfRank = df.withColumn("row",row_number.over(w1)).where(col("row") <= 5).drop("row")
        dfRank.groupBy("msisdn").agg(collect_list("corresp_msisdn").alias("top_appel"))
      }





      def dailyClients(table: String, fin: String): DataFrame = {
        spark.sql(
          s"""
             |SELECT
             |    TRIM(msisdn)                  AS msisdn                    ,
             |    TRIM(age)                     AS age                       ,
             |    TRIM(sex)                     AS sex                       ,
             |    TRIM(segment_marche)          AS segment_marche
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
             |          TRIM(msisdn)         AS  msisdn                    ,
             |          TRIM(ca_cr_commune)  AS  ca_cr_commune_day         ,
             |          COUNT(*)             AS  count
             |FROM $table
             |WHERE day BETWEEN '$debut' AND '$fin'
             |GROUP BY msisdn, ca_cr_commune_day
                     """.stripMargin)
        df.withColumn("row1",row_number.over(w1)).where(col("row1") === 1).drop("row1")
      }



      def locationNighttimeDf(table: String, debut: String, fin: String): DataFrame = {
        val w1 = Window.partitionBy("msisdn").orderBy(desc("count"))
        val df = spark.sql(
          s"""
             |SELECT
             |          TRIM(msisdn)         AS msisdn                         ,
             |          TRIM(ca_cr_commune)  AS ca_cr_commune_night            ,
             |          COUNT(*)             AS count
             |FROM $table
             |WHERE day BETWEEN '$debut' AND '$fin'
             |GROUP BY msisdn, ca_cr_commune_night
                         """.stripMargin)

        df.withColumn("row1",row_number.over(w1)).where(col("row1") === 1).drop("row1")
      }



      def usageDataDf(table: String, debut: String, fin: String): DataFrame = {
        spark.sql(
          s"""
             |SELECT TRIM(msisdn)                   AS msisdn                               ,
             |       SUM(data_volume)              AS  usage_data_90j
             |FROM $table
             |WHERE day BETWEEN '$debut' AND '$fin'
             |GROUP BY msisdn
                         """.stripMargin)
      }

      def tableFinalDf1(dualSimDf: DataFrame, traficVoixSmsDf: DataFrame, dailyClients: DataFrame, locationDaytimeDf: DataFrame, locationNighttimeDf: DataFrame, usageDataDf: DataFrame, topAppel: DataFrame): DataFrame = {


          //val w1 = Window.partitionBy(locationDaytimeDf("msisdn"), locationDaytimeDf("ca_cr_commune_day")).orderBy(desc("count"))
          //val w2 = Window.partitionBy(locationNighttimeDf("msisdn")).orderBy(locationNighttimeDf("ca_cr_commune_night"))
          /*df.withColumn("row",row_number.over(w1))
            .where($"row" === 1).drop("row")
            .show()*/

          dualSimDf
            .join(traficVoixSmsDf       , Seq("msisdn")   , "left")
            .join(dailyClients          , Seq("msisdn")   , "left")
            .join(locationDaytimeDf     , Seq("msisdn")   , "left")
            .join(locationNighttimeDf, Seq("msisdn"), "left")
            .join(usageDataDf           , Seq("msisdn")   , "left")
            .join(topAppel           , Seq("msisdn")   , "left")
            .groupBy(dualSimDf("msisdn"), dualSimDf("imei"), dualSimDf("multi_sim"), dailyClients("age"), dailyClients("sex"), dailyClients("segment_marche"), locationDaytimeDf("ca_cr_commune_day"), locationNighttimeDf("ca_cr_commune_night"), usageDataDf("usage_data_90j"), topAppel("top_appel"))
            //.groupBy(dualSimDf("msisdn"), dualSimDf("imei"), dualSimDf("multi_sim"), dailyClients("age"), dailyClients("sex"), dailyClients("segment_marche"), locationDaytimeDf("ca_cr_commune_day"), locationNighttimeDf("ca_cr_commune_night"))
            .agg(
              sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "ORA"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("nombre_appel_vers_orange_90j")       ,
              sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "TIG"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("nombre_appel_vers_free_90j")         ,
              sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "EXM"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("nombre_appel_vers_expresso_90j")     ,
              sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "ORA"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("nombre_min_appel_vers_orange_90j")   ,
              sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "TIG"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("nombre_min_appel_vers_free_90j")     ,
              sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "EXM"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("nombre_min_appel_vers_expresso_90j")
              //sum(usageDataDf("data_volume")).alias("usage_data_90j")
            )
            //.withColumn("row1",row_number.over(w1)).where(col("row1") === 1)//.drop("row1")
            //.withColumn("row2",row_number.over(w2))
            //.where(col("row2") === 1)
            // 345.995
            //.drop("row1", "row2")
            .select(
              dualSimDf("msisdn")                                      ,
              dualSimDf("imei")                                        ,
              dualSimDf("multi_sim")                                   ,
              dailyClients("age")                                      ,
              dailyClients("sex")                                      ,
              dailyClients("segment_marche")                           ,
              col("nombre_appel_vers_orange_90j")            ,
              col("nombre_appel_vers_free_90j")              ,
              col("nombre_appel_vers_expresso_90j")          ,
              col("nombre_min_appel_vers_orange_90j")        ,
              col("nombre_min_appel_vers_free_90j")          ,
              col("nombre_min_appel_vers_expresso_90j")      ,
              locationDaytimeDf("ca_cr_commune_day")                   ,
              locationNighttimeDf("ca_cr_commune_night")               ,
              usageDataDf("usage_data_90j")                            ,
              topAppel("top_appel")
            ).coalesce(100)
        }






}
