//import fonctions.constants
import fonctions.utils
import org.apache.spark.sql.DataFrame

object MultiSim {

  def main(args: Array[String]): Unit = {

    val startTime = System.nanoTime()

    val debut = args(0)
    val fin = args(1)


    /*val dualSim = constants.spark.sql(
      s"""SELECT
         |dual_sim.caller_msisdn
         |COUNT(CASE WHEN voix_sms.op_called_msisdn = '' THEN 1 ELSE NULL END) AS Nbre_appel_orange ,
         |COUNT(CASE WHEN voix_sms.op_called_msisdn = '' THEN 1 ELSE NULL END) AS Nbre_appel_free ,
         |COUNT(CASE WHEN voix_sms.op_called_msisdn = '' THEN 1 ELSE NULL END) AS Nbre_appel_expresso ,
         |FROM refined_seter.dataset_duallsim dual_sim
         |LEFT JOIN refined_trafic.trafic_voix_sms voix_sms
         |ON dual_sim.equipment_identity = voix_sms.equipment_identity""".stripMargin
    )*/

    //val df = utils.sicoDf("refined_trafic.trafic_voix_sms", debut, fin)

    /*val df = constants.spark.sql(
      s"""
         |SELECT * FROM refined_trafic.trafic_voix_sms
         |WHERE day BETWEEN '$debut' AND '$fin' limit 5
       """.stripMargin
    )*/

    val dual_sim_df = utils.dualSimDf("refined_seter.dataset_duallsim")
    val trafic_voix_sms_df = utils.traficVoixSmsDf("refined_trafic.trafic_voix_sms", debut, fin)
    val sico_df = utils.sicoDf("trusted_sicli.sico")
    val segment_marche_df = utils.segmentMarcheDf("refined_reporting.segment_marche")
    val location_daytime_df  = utils.locationDaytimeNightimeDf("refined_localisation.location_daytime", debut, fin)
    val location_nightime_df = utils.locationDaytimeNightimeDf("refined_localisation.location_nighttime", debut, fin)
    val usage_data_df = utils.usageDataDf("refined_trafic.trafic_data", debut, fin)
    //val df_final = utils.tableFinalDf(dual_sim_df, trafic_voix_sms_df, location_daytime_df)

    //sico_df.select("month").distinct().show()

    //df.show(false)

    dual_sim_df.show(5, false)
    trafic_voix_sms_df.show(5, false)
    sico_df.show(5, false)
    segment_marche_df.show(5, false)
    location_daytime_df.show(5, false)
    location_nightime_df.show(5, false)
    usage_data_df.show(5, false)

    println("-----------------------------------------------------")


    println("dual_sim: "+dual_sim_df.count())
    println("trafic_voix_sms: "+trafic_voix_sms_df.count())
    println("sico: "+sico_df.count())
    println("segment_marche: "+segment_marche_df.count())
    println("location_daytime: "+location_daytime_df.count())
    println("location_nighttime: "+location_nightime_df.count())
    println("usage_data: "+usage_data_df.count())

    println("---------------------------------------------------")
    //df_final.show(5, false)
    println(debut)
    println(fin)

    println("----------------------------------------------------")
    // Capturer le temps de fin
    val endTime = System.nanoTime()

    // Calculer le temps écoulé en minutes
    val durationMinutes = (endTime - startTime) / 60000000000.0

    // Afficher le temps d'exécution
    println(s"Temps d'exécution : $durationMinutes minutes")
  }




            /*
                                dual_sim: 345.995
                                trafic_voix_sms: 3.169.074.275
                                sico: 16.931.958
                                segment_marche: 173.404.892
                                location_daytime: 869.407.774
                                location_nighttime: 706.476.228
                                usage_data: 11.735.153.984
             */
}
