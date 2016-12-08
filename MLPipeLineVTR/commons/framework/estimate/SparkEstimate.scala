package commons.framework.estimate

import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2016/12/8.
  */
abstract class SparkEstimate extends Estimate{
  type SOURCE
  var originRDD: RDD[SOURCE]

}
