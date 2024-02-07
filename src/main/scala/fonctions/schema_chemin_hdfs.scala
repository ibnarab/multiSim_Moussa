package fonctions

import java.util.Calendar

import org.apache.spark.sql.types._

object schema_chemin_hdfs {

  /*

      Age, sexe, segment de marché,
      localisation jour , localisation nuit
      Nbre de contact vers Orange, Expresso, Free. ( sur les 3 derniers mois )
      Nbre de minutes d'appels vers Orange, Expresso, Free ( sur les 3 derniers mois )
      Usage data (sur les 3 derniers mois )
      Top 5 contact de chaque client  (sur les 3 derniers mois )

   */

  // Age, sexe => trusted_sicli.sico (Age: date_naissance, sexe: numero_piece)

  //segment de marche => refined_reporting.segment_marche, refined_trafic.master_data (data_marche, segment_agr)

  // localisation jour , localisation nuit => refined_localisation.{location_daytime, location_nighttime} je prends quelles colonnes, yen a bcp?

  // Nbre d'appel de contact vers Orange, Expresso, Free  ( sur les 3 derniers mois ) op_called_msisdn : calcul du trafic refined_trafic.trafic_voix_sms par operateur aggréger, la colonne destination, mais ya des pays aussi, nombre d'appels oubien?

  // Nbre de minutes d'appels vers Orange, Expresso, Free ( sur les 3 derniers mois ): calcul du trafic voix_sms par operateur aggréger par minutes, nombre ou montant? en seconde

  // Usage data: refined_trafic.trafic_data, montant? en octets periode 3 derniers mois

  // Top 5 contact de chaque client  (sur les 3 derniers mois ) ???? [77%][77%] (top operateur?)

  /*
        table trusted_cdr.cdrnm, refined_trafic.trafic_voix_sms(droite), refined_trafic.trafic_data, refined_trafic.master_data, refined_trafic.terminaux

        select * from refined_seter.dataset_duallsim limit 5 gauche

        select * from refined_trafic.trafic_voix_sms where year=2023 limit 5
   */



}
