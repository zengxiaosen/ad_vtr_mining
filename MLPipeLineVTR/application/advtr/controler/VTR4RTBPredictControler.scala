package application.advtr.controler

import application.advtr.sample.handler._
import application.advtr.sample.{VTRSampleConfig, VTRSampleMaker}
import com.esotericsoftware.kryo.Kryo
import model.XgboostTreeModel
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by Administrator on 2016/12/8.
  */
class VTR4RTBPredictControler {

}

object VTR4RTBPredictControler{
  class Register extends KryoRegistrator{
    override def registerClasses(kryo: Kryo): Unit = {
      kryo.register(classOf[LabeledPoint])
      kryo.register(classOf[XgboostTreeModel])
    }
  }
  var parentPath = ""
  var sc: SparkContext = _

  def runETLAndTrainByLogisticSGD(inputPath: String, formatDataOutputPath: String, modelPatch: String, testResultPath: String, local_analyzePath: String): Unit ={
    val training = sc.textFile(inputPath)//.coalesce(840,true)
    //val training = sc.textFile("file:///opt/ssy/workspace/vtr/data/orgSample/*")//.coaleasce(840.true)
    //feature handler register
    val featureIndex = init()
    val sampleMaker = VTRSampleMaker.instance

    val dataInstanceTransformer = new


  }

  def init(): Int = {
    VTRSampleConfig.loadConf(parentPath)

    var featureIndex = 0
    val sampleMaker = VTRSampleMaker.instance

    if(VTRSampleConfig.timestampHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new TimeStampHandler, featureIndex)
      System.out.println(s"TimeStampHandler init succeed, featureIndex:$featureIndex")

    }
    if(VTRSampleConfig.timeChannelHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new TimeChannelHandler, featureIndex)
      System.out.println(s"TimeChannelHandler init succeed,featureIndex:$featureIndex")
    }
    if(VTRSampleConfig.timeClientTypeHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new TimeClientTypeHandler, featureIndex)
      System.out.println(s"TimeClientTypeHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.timeDeviceTypeHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new TimeDeviceTypeHandler, featureIndex)
      System.out.println(s"TimeDeviceTypeHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.timeOSTypeHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new TimeOSTypeHandler, featureIndex)
      System.out.println(s"TimeOSTypeHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.channelHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new ChannelHandler, featureIndex)
      System.out.println(s"ChannelHandler init succeed,featureIndex:$featureIndex")
    }
    if(VTRSampleConfig.osTypeHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new OsTypeHandler, featureIndex)
      System.out.println(s"OsTypeHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.deviceTypeHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new DeviceTypeHandler, featureIndex)
      System.out.println(s"DeviceTypeHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.ideaLengthHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new IdeaLengthHandler, featureIndex)
      System.out.println(s"IdeaLengthHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.ideaIndustryAndSubHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new IdeaIndustryAndSubHandler, featureIndex)
      System.out.println(s"ideaIndustryAndSubHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.prefAdIndustryMatchedHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new PrefAdIndustryMatchedHandler, featureIndex)
      System.out.println(s"PrefAdIndustryMatchedHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.userAdPrefHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new UserAdPrefHandler, featureIndex)
      System.out.println(s"UserAdPrefHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.prefChannelMatchedHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new PrefChannelMatchedHandler, featureIndex)
      System.out.println(s"PrefChannelMatchedHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.userChannelPrefHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new UserChannelPrefHandler, featureIndex)
      System.out.println(s"UserChannelPrefHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.sexHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new SexHandler, featureIndex)
      System.out.println(s"SexHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.sexIdeaIndustryAndSubHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new SexIdeaIndustryAndSubHandler, featureIndex)
      System.out.println(s"SexIdeaIndustryAndSubHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.ageHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new AgeHandler, featureIndex)
      System.out.println(s"AgeHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.ageIdeaIndustryAndSubHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new AgeIdeaIndustryAndSubHandler, featureIndex)
      System.out.println(s"AgeIdeaIndustryAndSubHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.sexAgeHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new SexAgeHandler, featureIndex)
      System.out.println(s"SexAgeHandler init succeed,featureIndex:$featureIndex")
    }
    if(VTRSampleConfig.ideaHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new IdeaHandler, featureIndex)
      System.out.println(s"IdeaHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.ideaVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new IdeaVTRHandler, featureIndex)
      System.out.println(s"IdeaVTRHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.adxPIdClientTypeHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new AdxPIdClientTypeHandler, featureIndex)
      System.out.println(s"adxPIdClientTypeHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.adxPIdClientTypeVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new AdxPIdClientTypeVTRHandler, featureIndex)
      System.out.println(s"adxPIdClientTypeVTRHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.cityVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new CityVTRHandler, featureIndex)
      System.out.println(s"CityVTRHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.cityHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new CityHandler, featureIndex)
      System.out.println(s"CityHandler init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.videoIdHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new VideoIdHandler, featureIndex)
      System.out.println(s"VideoIdHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.videoIdVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new VideoIdVTRHandler, featureIndex)
      System.out.println(s"videoIdVTRHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.videoIdIdeaIdHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new VideoIdIdeaIdHandler, featureIndex)
      System.out.println(s"videoIdIdeaIdHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.videoIdIdeaIdVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new VideoIdIdeaIdVTRHandler, featureIndex)
      System.out.println(s"videoIdIdeaIdVTRHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.subChannelHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new SubChannelHandler, featureIndex)
      System.out.println(s"subChannelHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.subChannelVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new SubChannelVTRHandler, featureIndex)
      System.out.println(s"subChannelVTRHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.subChannelAdSubIndustryHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new SubChannelAdSubIndustryHandler, featureIndex)
      System.out.println(s"subChannelAdSubIndustryHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.subChannelAdSubIndustryVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new SubChannelAdSubIndustryVTRHandler, featureIndex)
      System.out.println(s"subChannelAdSubIndustryVTRHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.channelAdIndustryHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new ChannelAdIndustryHandler, featureIndex)
      System.out.println(s"channelAdIndustryHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.channelAdIndustryVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new ChannelAdIndustryVTRHandler, featureIndex)
      System.out.println(s"channelAdIndustryVTRHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.adxPIdSubChannelHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new AdxPIdSubChannelHandler, featureIndex)
      System.out.println(s"adxPIdSubChannelHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.adxPIdSubChannelVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new AdxPIdSubChannelVTRHandler, featureIndex)
      System.out.println(s"adxPIdSubChannelVTRHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.adxPIdChannelHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new AdxPIdChannelHandler, featureIndex)
      System.out.println(s"adxPIdChannelHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    if(VTRSampleConfig.adxPIdChannelVTRHandlerSwitch){
      featureIndex = sampleMaker.registerFeatureHandler(new AdxPIdChannelVTRHandler, featureIndex)
      System.out.println(s"adxPIdChannelVTRHandlerSwitch init succeed,featureIndex:$featureIndex")
    }

    featureIndex
  }


}
