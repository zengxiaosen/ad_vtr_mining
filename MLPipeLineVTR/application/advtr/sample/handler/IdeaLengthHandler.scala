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
 * Created by leilongyan on 2015/5/12.
 * 素材时长，以秒s为单位，5s为一段
 * 注意的是：时长0表示暂停
 */
class IdeaLengthHandler(val featureHandlerName: String = "IdeaLengthHandler") extends FeatureHandler {
  private val ideaLengthFeatureConf = VTRSampleConfig.parentPath + "featureHandlerConf/cpv_ideaLength.conf"
  private var maxFeatureIndex = 0
  private val numOfField = 2
  private var ideaLengthFeatureMap: Map[String, Feature] = new HashMap[String, Feature]()
  private var unSeenFeature: Feature = _
  private val discreteDis = VTRSampleConfig.ideaLengthDis

  override def initFeatureHandler(initFeatureIndex: Int): Int = {
    var source: BufferedSource = null
    try {
      source = Source.fromFile(ideaLengthFeatureConf)
      val linesItr = source.getLines()
      for (line <- linesItr) {
        val contents = line.split("\\t")
        if (contents.length == numOfField) {
          if (!ideaLengthFeatureMap.contains(contents(0).trim)) {
            val localIndex = contents(1).trim.toInt
            ideaLengthFeatureMap.+=(contents(0).trim -> Feature(localIndex + initFeatureIndex, "cpv_ideaLength", 1.0))
          }
        }
      }
    } catch {
      case ex: IOException => ex.printStackTrace()
    } finally {
      maxFeatureIndex = initFeatureIndex + ideaLengthFeatureMap.size + 1
      unSeenFeature = Feature(maxFeatureIndex, "ideaLength", 1.0)
      for((propertyValue,feature) <- ideaLengthFeatureMap.toArray.sortBy(_._2.featureIndex)){
        System.out.println(s"feature:${feature.featureIndex},[${feature.featureName}]_[$propertyValue]")
      }
      System.out.println(s"feature:${maxFeatureIndex},[ideaLength]_[none]")
      if (source != null) {
        source.close()
      }
    }
    maxFeatureIndex
  }

  override def extractFeature(dataInstance: DataInstance): ArrayBuffer[Feature] = {
    val features = new ArrayBuffer[Feature](1)
    if (dataInstance.isInstanceOf[VTRDataInstance]) {
      val ideaLength = dataInstance.asInstanceOf[VTRDataInstance].ideaLength
      var il = Int.MaxValue
      if(ideaLength != -1.0){
        if(ideaLength.toInt == 0){ //时长0表示暂停
          il = 0
        }else{
          il = ideaLength.toInt/discreteDis + 1
        }
      }
      if (ideaLengthFeatureMap.contains(il.toString)) {
        features.+=(ideaLengthFeatureMap.get(il.toString).get)
      } else {
        features.+=(unSeenFeature)
      }
    }
    features
  }
}
