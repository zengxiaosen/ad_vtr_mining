package application.advtr.data

import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by Administrator on 2016/12/8.
  */
case class MyLabeledPoint (val posId: String, val clientType: String, var labeledPoint: LabeledPoint){

}
