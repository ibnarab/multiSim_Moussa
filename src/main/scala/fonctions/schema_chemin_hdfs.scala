package fonctions

object schema_chemin_hdfs {


        val table_dualsim               = "refined_seter.dataset_duallsim"

        val table_trafic_voix_sms       = "refined_trafic.trafic_voix_sms"

        val table_daily_clients         = "refined_vue360.daily_clients"

        val table_location_daytime      = "refined_localisation.location_daytime"

        val table_location_nighttime    = "refined_localisation.location_nighttime"

        val table_trafic_data           = "refined_trafic.trafic_data"

        val chemin_table_finale         = "/dlk/osn/refined/Reporting/multisim"

        val table_finale                = "refined_reporting.multisim"

        val compression                 = "gzip"

        val header                      =  true

        val table_finale_mysql          = "MYSVENTEREBONDPRD.multisim_total"

}
