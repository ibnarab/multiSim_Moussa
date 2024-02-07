import fonctions.constants

object MultiSim {

  def main(args: Array[String]): Unit = {

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

    val df = constants.spark.sql(
      s"""
         |SELECT * FROM refined_trafic.trafic_voix_sms
         |WHERE day BETWEEN '$debut' AND '$fin' limit 5
       """.stripMargin
    )

    df.select("month").distinct().show()

    df.show(false)

    println(debut)
    println(fin)
  }

}
