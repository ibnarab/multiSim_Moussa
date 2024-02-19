import fonctions.{utils, read_write, schema_chemin_hdfs}

object MultiSim {

  def main(args: Array[String]): Unit = {


            val debut = args(0)

            val fin = args(1)



            val dual_sim_df             = utils.dualSimDf           (schema_chemin_hdfs.table_dualsim                                )

            val trafic_voix_sms_df      = utils.traficVoixSmsDf     (schema_chemin_hdfs.table_trafic_voix_sms       , debut   ,   fin)

            val daily_clients           = utils.dailyClients        (schema_chemin_hdfs.table_daily_clients         ,             fin)

            val location_daytime_df     = utils.locationDaytimeDf   (schema_chemin_hdfs.table_location_daytime      , debut   ,   fin)

            val location_nightime_df    = utils.locationNighttimeDf (schema_chemin_hdfs.table_location_nighttime    , debut   ,   fin)

            val usage_data_df           = utils.usageDataDf         (schema_chemin_hdfs.table_trafic_data           , debut   ,   fin)

            val top_appel               = utils.topAppel            (schema_chemin_hdfs.table_trafic_voix_sms       , debut   ,   fin)



            val df_final                = utils.tableFinalDf1       (

              dual_sim_df                               ,

              trafic_voix_sms_df                        ,

              daily_clients                             ,

              location_daytime_df                       ,

              location_nightime_df                      ,

              usage_data_df                             ,

              top_appel

            )



            read_write.writeHiveMlutiSim                            (

              df_final                                  ,

              schema_chemin_hdfs.header                 ,

              schema_chemin_hdfs.compression            ,

              schema_chemin_hdfs.chemin_table_finale    ,

              schema_chemin_hdfs.table_finale


            )

  }


}
