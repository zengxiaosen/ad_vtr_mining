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
class AgeHandler(val featureHandlerName: String = "AgeHandler") extends FeatureHandler{
  private val ageFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_userAge.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var ageFeatureMap: Map[String,Feature] = new HashMap[String,Feature]()
  private var unSeenFeature: Feature = _

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try{
      source = Source.fromFile(ageFeatureConf)
      val linesItr = source.getLines()
      for(line <- linesItr){
        val contents = line.split("\\t")
        if(contents.length == numOfField){
          if(!ageFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            ageFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "age", 1.0))
          }
        }
      }
    }catch {
      case ex: IOException => ex.printStackTrace()
    }finally {
      maxFeatureIndex = initFeatureIndex + ageFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "age", 1.0)
      for((propertyValue,feature) <- ageFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[age]_[none]")
      if(source != null){
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    val features = new ArrayBuffer[Feature](1)
    if(dataInstance.isInstanceOf[VTRDataInstance]){
      val age = dataInstance.asInstanceOf[VTRDataInstance].userAge
      if(ageFeatureMap.contains(age)){
        features.+=(ageFeatureMap.get(age).get)
      }else{
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
