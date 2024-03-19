package fonctions

import org.apache.spark.sql.SparkSession

object constants {

        val spark =

          SparkSession.builder

          .appName("multisim")

          .config("spark.hadoop.fs.defaultFS", "hdfs://bigdata")

          .config("spark.master", "yarn")

          .config("spark.submit.deployMode", "cluster")

          .enableHiveSupport()

          .getOrCreate()


        val mode_overwrite                    =     "overwrite"

}
