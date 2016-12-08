package application.advtr.sample.handler

/**
 * Created by shishuyuan on 2015/11/14.
 */

import java.io.IOException

import application.advtr.data.VTRDataInstance
import application.advtr.sample.VTRSampleConfig
import commons.framework.data.DataInstance
import commons.framework.sample.{Feature, FeatureHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
 * Created by leilongyan on 2015/5/21.
 */
class AdxPIdClientTypeHandler(val featureHandlerName: String = "adxPIdClientTypeHandler") extends FeatureHandler{
  private val adxPIdClientTypeConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_adxPId_clientType.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var adxPIdClientTypeFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(adxPIdClientTypeConf)
      val linesItr = source.getLines()
      for(line <- linesItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!adxPIdClientTypeFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            adxPIdClientTypeFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "adxPIdClientType", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + adxPIdClientTypeFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "adxPIdClientType", 1.0)
      for((propertyValue,feature) <- adxPIdClientTypeFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[adxPIdClientType]_[none]")
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
      val adxPId= VTRInstance.adxPId
      val adxPIdClientType = adxPId+ "_" + clientType
      if(adxPIdClientTypeFeatureMap.contains(adxPIdClientType)){
        features.+=(adxPIdClientTypeFeatureMap.get(adxPIdClientType).get)
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}

