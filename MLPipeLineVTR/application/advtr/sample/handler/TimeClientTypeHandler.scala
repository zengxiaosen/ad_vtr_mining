package application.advtr.sample.handler

import java.io.IOException
import java.util.Calendar

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
  * Created by Administrator on 2016/12/8.
  */
class TimeClientTypeHandler (val featureHandlerName: String = "TimeClientTypeHandler") extends FeatureHandler{
  private val timeClientTypeFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_timeStamp_clientType.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var timeClientTypeFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _
  var calendar: Calendar = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      calendar = Calendar.getInstance()
      source = Source.fromFile(timeClientTypeFeatureConf)
      val linesItr = source.getLines()
      for(line <- linesItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!timeClientTypeFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            timeClientTypeFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "timeClientType", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + timeClientTypeFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "timeClientType", 1.0)
      for((propertyValue,feature) <- timeClientTypeFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[timeClientType]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    val features = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val VTRInstance = dataInstance.asInstanceOf[VTRDataInstance]
      val clientType = VTRInstance.clientType
      val timeStamp = VTRInstance.timeStamp
      calendar.setTimeInMillis(timeStamp)
      val hour = calendar.get(Calendar.HOUR_OF_DAY)
      val timeClientType = hour + "_" + clientType
      if(timeClientTypeFeatureMap.contains(timeClientType)){
        features.+=(timeClientTypeFeatureMap.get(timeClientType).get)
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}

