import fonctions.constants._
import fonctions.{constants, utils, read_write}
import org.apache.spark.sql.{DataFrame, SaveMode}

object MultiSim {

  def main(args: Array[String]): Unit = {


            val debut = args(0)
            val fin = args(1)

    //println(df.count())




            val dual_sim_df             = utils.dualSimDf("refined_seter.dataset_duallsim")

            val trafic_voix_sms_df      = utils.traficVoixSmsDf("refined_trafic.trafic_voix_sms", debut, fin)

            val daily_clients = utils.dailyClients("refined_vue360.daily_clients", fin)
            //trafic_voix_sms_df.createOrReplaceTempView("trafic_voix_sms_df")
            //val sico_df                 = utils.sicoDf("trusted_sicli.sico")
            //sico_df.createOrReplaceTempView("sico_df")

            //segment_marche_df.createOrReplaceTempView("segment_marche_df")
            val location_daytime_df     = utils.locationDaytimeDf("refined_localisation.location_daytime", debut, fin)
            //location_daytime_df.createOrReplaceTempView("location_daytime_df")
            val location_nightime_df    = utils.locationNighttimeDf("refined_localisation.location_nighttime", debut, fin)
            //location_nightime_df.createOrReplaceTempView("location_nightime_df")
            val usage_data_df           = utils.usageDataDf("refined_trafic.trafic_data", debut, fin)
            //usage_data_df.createOrReplaceTempView("usage_data_df")
            val top_appel = utils.topAppel("refined_trafic.trafic_voix_sms", debut, fin)
            val df_final                  = utils.tableFinalDf1(dual_sim_df, trafic_voix_sms_df, daily_clients, location_daytime_df, location_nightime_df, usage_data_df, top_appel)
            /*df_final.show(5, false)
            println(df_final.count())*/
            //df_final.printSchema()

            read_write.writeHiveMlutiSim(df_final, true, "/dlk/osn/refined/Reporting/dualsim", ",refined_reporting.dual_sim")


            /*df_final.write
              .mode(SaveMode.Overwrite)
              .partitionBy("year", "month", "day") // Spécifier les colonnes de partition
              .option("header", true)
              .option("compression", "gzip")
              //.option("path", chemin)
              .saveAsTable("refined_reporting.multiSimTest2")*/
            //val df_final2                 = utils.tableFinalDf2(df_final, sico_df)
            //df_final.coalesce(100)
            //println(df_final.rdd.getNumPartitions)
            //println(df_final2.rdd.getNumPartitions)
            /*df_final.cache()
            df_final.count()
            val df_final2                 = utils.tableFinalDf2(df_final, location_daytime_df)*/
            //val df_final2                  = utils.tableFinalDf2(df_final, location_nightime_df)

            //sico_df.select("month").distinct().show()

            //df_final.show(false)

            /*dual_sim_df.show(5, false)
            trafic_voix_sms_df.show(5, false)
            sico_df.show(5, false)
            segment_marche_df.show(5, false)
            location_daytime_df.show(5, false)
            location_nightime_df.show(5, false)
            usage_data_df.show(5, false)*/
            //df_final.show(5, false)
            //df_final2.show(5, false)

          /*df_final.write
            .mode(SaveMode.Overwrite)
            //.partitionBy("year", "month", "day") // Spécifier les colonnes de partition
            .option("header", true)
            .option("compression", "gzip")
            //.option("path", chemin)
            .saveAsTable("refined_reporting.multiSimTest")*/

            //println("df_final: "+df_final.count())

            println("-----------------------------------------------------")


            /*println("dual_sim: "+dual_sim_df.count())
            println("trafic_voix_sms: "+trafic_voix_sms_df.count())
            println("df_final: "+df_final.count())*/
            /*println("segment_marche: "+segment_marche_df.count())
            println("location_daytime: "+location_daytime_df.count())
            println("location_nighttime: "+location_nightime_df.count())
            println("usage_data: "+usage_data_df.count())*/
            //println("table finale: "+df_final.count())*/

  }




            /*
                                dual_sim: 345.995
                                trafic_voix_sms: 10.538.745.442
                                sico: 17.006.341
                                segment_marche: 173.404.892
                                location_daytime: 869.407.774
                                location_nighttime: 706.476.228
                                usage_data: 11.735.153.984
             */

            /*
                                dual_sim: 345.995
                                trafic_voix_sms: 24.496.591
                                sico: 3.511.658
                                segment_marche: 22.280.806
                                location_daytime: 314.228.551
                                location_nighttime: 209.673.023
                                usage_data: 22.007.772
             */

            /*
                  dual_sim: 345.995
                  trafic_voix_sms: 6.905.798
                  sico: 15.290.953
                  segment_marche: 22.280.806
                  location_daytime: 13.144.907
                  location_nighttime: 9.880.078
                  usage_data: 7546741
             */
}
