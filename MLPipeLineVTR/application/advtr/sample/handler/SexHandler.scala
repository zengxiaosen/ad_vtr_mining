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
 * Created by leilongyan on 2015/4/28.
 */
class SexHandler(val featureHandlerName: String = "SexHandler") extends FeatureHandler{
  private val sexFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_userSex.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var sexFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(sexFeatureConf)
      val linesItr = source.getLines()
      for(line <- linesItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!sexFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            sexFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "sex", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + sexFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "sex", 1.0)
      for((propertyValue,feature) <- sexFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[sex]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    val features = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val sex = dataInstance.asInstanceOf[VTRDataInstance].userSex
      if(sexFeatureMap.contains(sex)){
        features.+=(sexFeatureMap.get(sex).get)
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
