package application.advtr.sample.handler

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

class OsTypeHandler(val featureHandlerName: String = "OsTypeHandler") extends FeatureHandler{
  private val osTypeFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_osType.conf"
  private val numOfField = 2
  private var maxFeatureIndex = 0
  private var osTypeFeaturesMap: Map[String, Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(osTypeFeatureConf)
      val lineItr = source.getLines()
      for(line <- lineItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!osTypeFeaturesMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            osTypeFeaturesMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "cpv_osType", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + osTypeFeaturesMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "osType", 1.0)
      for((propertyValue,feature) <- osTypeFeaturesMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[osType]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    var features: ArrayBuffer[Feature] = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val osType = (dataInstance.asInstanceOf[VTRDataInstance]).osType
      if(osTypeFeaturesMap.contains(osType)){
        features.+=(osTypeFeaturesMap(osType))
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
