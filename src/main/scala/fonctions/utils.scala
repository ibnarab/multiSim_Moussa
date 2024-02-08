package fonctions

import org.apache.spark.sql.DataFrame
import constants._
import org.apache.spark.sql.functions._


object utils {

      def dualSimDf(table: String): DataFrame = {
        spark.sql(
          s"""
             |SELECT
             |   equipment_identity AS imei
             |FROM $table
           """.stripMargin
        )
      }

      def traficVoixSmsDf(table: String, debut: String, fin: String): DataFrame = {
          spark.sql(
            s"""
               |SELECT
               |    caller_msisdn                                                                         AS msisdn                          ,
               |    equipment_identity                                                                    AS imei                            ,
               |    COUNT(CASE WHEN      op_called_msisdn   = 'ORA' THEN 1               ELSE NULL END)   AS nombre_appel_vers_orange        ,
               |    COUNT(CASE WHEN      op_called_msisdn   = 'TIG' THEN 1               ELSE NULL END)   AS nombre_appel_vers_free          ,
               |    COUNT(CASE WHEN      op_called_msisdn   = 'EXM' THEN 1               ELSE NULL END)   AS nbre_appel_vers_expresso        ,
               |    SUM(CASE WHEN        op_called_msisdn   = 'ORA' THEN dureeappel / 60 ELSE 0    END)   AS nombre_min_appel_vers_orange    ,
               |    SUM(CASE WHEN        op_called_msisdn   = 'TIG' THEN dureeappel / 60 ELSE 0    END)   AS nombre_min_appel_vers_free      ,
               |    SUM(CASE WHEN        op_called_msisdn   = 'EXM' THEN dureeappel / 60 ELSE 0    END)   AS nbre_min_appel_vers_expresso
               |FROM $table
               |WHERE
               |    day BETWEEN '$debut' AND '$fin'
               |AND
               |    traffic_direction = "SORTANT" AND op_called_msisdn IN ('ORA', 'TIG', 'EXM') AND traffic_type = "VOIX"
               |GROUP BY
               |    caller_msisdn, equipment_identity
             """.stripMargin)
      }

      def sicoDf(table: String): DataFrame = {
        val df = spark.sql(
          s"""
             |SELECT DISTINCT
             |  nd                                                                          As msisdn   ,
             |  CONCAT(CAST(year(current_date()) - year(date_naissance) AS STRING), ' ans') AS age      ,
             |  CASE WHEN SUBSTR(numero_piece, 1, 1) = '1' THEN 'Homme'
             |       WHEN SUBSTR(numero_piece, 1, 1) = '2' THEN 'Femme'
             |       ELSE NULL
             |  END                                                                         AS sexe
             |FROM $table
             |WHERE numero_piece LIKE '%1' OR numero_piece LIKE '%2'
     """.stripMargin)

        df
      }



      def segmentMarcheDf(table: String): DataFrame = {
        spark.sql(
          s"""
             |SELECT DISTINCT
             |    msisdn          ,
             |    segment_marche
             |FROM $table
                     """.stripMargin)
      }

      def locationDaytimeNightimeDf(table: String, debut: String, fin: String): DataFrame = {
        spark.sql(
          s"""
             |SELECT DISTINCT
             |          msisdn                    ,
             |          nom_cellule               ,
             |          region                    ,
             |          departement               ,
             |          commune_arrondissement    ,
             |          ca_cr_commune             ,
             |          nom_site
             |FROM $table
             |WHERE day BETWEEN '$debut' AND '$fin'
                     """.stripMargin)
      }

      def usageDataDf(table: String, debut: String, fin: String): DataFrame = {
        spark.sql(
          s"""
             |SELECT msisdn                             ,
             |       equipment_identity                 ,
             |       sum(data_volume) AS data_volume
             |FROM $table
             |WHERE
             |      day BETWEEN '$debut' AND '$fin'
             |GROUP BY msisdn, equipment_identity
                     """.stripMargin)
      }

  /*def tableFinalDf(dualSimDf: DataFrame, traficVoixSmsDf: DataFrame, sicoDf: DataFrame, segmentMarcheDf: DataFrame, locationDaytimeDf : DataFrame, locationNightimeDf: DataFrame, usageDataDf: DataFrame): DataFrame = {

        dualSimDf
          .join(traficVoixSmsDf, dualSimDf("imei") === traficVoixSmsDf("imei"), "left")
          .join(sicoDf, traficVoixSmsDf("msisdn") === sicoDf("msisdn"), "left")
          .join(segmentMarcheDf, sicoDf("msisdn") === segmentMarcheDf("msisdn"), "left")
          .join(locationDaytimeDf, segmentMarcheDf("msisdn") === locationDaytimeDf("msisdn"), "left")
          .join(locationNightimeDf, locationDaytimeDf("msisdn") === locationNightimeDf("msisdn"), "left")
          .join(usageDataDf, locationNightimeDf("msisdn") === usageDataDf("msisdn"), "left")
      }*/

        /*def tableFinalDf(dualSimDf: DataFrame, traficVoixSmsDf: DataFrame, locationDaytimeDf : DataFrame): DataFrame = {

        dualSimDf
          .join(traficVoixSmsDf, dualSimDf("equipment_identity") === traficVoixSmsDf("equipment_identity"), "left")
          .join(locationDaytimeDf, traficVoixSmsDf("caller_msisdn") === locationDaytimeDf("msisdn"), "left")
      }*/


}
