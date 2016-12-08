package application.advtr.sample.handler

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
 * Created by leilongyan on 2015/4/15.
 */
class DeviceTypeHandler(val featureHandlerName: String = "DeviceTypeHandler") extends FeatureHandler{
  private val deviceTypeFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_deviceType.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var deviceTypeFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(deviceTypeFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!deviceTypeFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            deviceTypeFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "cpv_deviceType", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + deviceTypeFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "deviceType", 1.0)
        for((propertyValue,feature) <- deviceTypeFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[deviceType]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val deviceType = (dataInstance.asInstanceOf[VTRDataInstance]).deviceType
      if(deviceTypeFeaturesMap.contains(deviceType)){
        features.+=(deviceTypeFeaturesMap(deviceType))
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
