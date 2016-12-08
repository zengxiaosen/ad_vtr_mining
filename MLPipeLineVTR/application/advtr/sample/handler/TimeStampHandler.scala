package application.advtr.sample.handler

import java.util.Calendar

import application.advtr.data.VTRDataInstance
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/12/8.
  */
class TimeStampHandler (val featureHandlerName: String = "TimeStampHandler") extends FeatureHandler{

  val unSeenFeatureIndex = 24
  var startFeatureIndex: Int = _
  var calendar: Calendar = _


  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    startFeatureIndex = initFeatureIndex
    calendar = Calendar.getInstance()

    var index = 0 // 时间这里是0开始
    var featureIndex = startFeatureIndex + 1
    while(featureIndex <= startFeatureIndex + unSeenFeatureIndex){
      System.out.println(s"feature:${featureIndex},[timestamp]_[$index]")
      featureIndex += 1
      index += 1
    }

    startFeatureIndex + unSeenFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    val features = new ArrayBuffer[Feature](1)
    try{
      if(dataInstance.isInstanceOf[VTRDataInstance]){
        val VTRDataInstance = dataInstance.asInstanceOf[VTRDataInstance]
        val timeStamp = VTRDataInstance.timeStamp
        calendar.setTimeInMillis(timeStamp)
        val hour = calendar.get(Calendar.HOUR_OF_DAY)
        features.+=(Feature(startFeatureIndex + hour + 1, "timestamp", 1.0))//hour是从0开始的

      }
    }catch {
      case e: Exception => println(e.printStackTrace())
        features.+=(Feature(startFeatureIndex + unSeenFeatureIndex, "timestamp", 1.0))
    }finally {

    }
    features
  }
}
