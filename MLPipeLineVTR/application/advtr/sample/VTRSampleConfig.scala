package application.advtr.sample

import java.io.{FileInputStream, IOException}
import java.util.Properties

/**
  * Created by Administrator on 2016/12/8.
  */
private class VTRSampleConfig {

}

object VTRSampleConfig{
  var parentPath = ""

  def loadConf(parentPath: String){
    this.parentPath = parentPath + "/"
    val confFile = this.parentPath + "LR4DspVTR.properties"
    try{
      val props = new Properties()
      val inputStream = new FileInputStream(confFile)
      props.load(inputStream)
      setParameters(props)
      inputStream.close()
    }catch{
      case ex: IOException => ex.printStackTrace()
    }
  }

  /**
    * label,site,time,provinceId,cityId,castId,cat,subcat,vid,effect, (10)
    * ideaLength,isLong(1,0),videoLength,adPosition,copyright,orderId,orderType, (7)
    * packageType,productType,avs,clearDegree,deviceType,osType,clientType,(7)
    * ideaId, ideaCat, ideaSubIndustry, ideaIndustry,cookie,adsPref,channelPref,sex,age (8)
    */
  var timestampHandlerSwitch = false
  var timeChannelHandlerSwitch = false
  var timeClientTypeHandlerSwitch = false
  var timeDeviceTypeHandlerSwitch = false
  var timeOSTypeHandlerSwitch = false
  var channelHandlerSwitch = false
  var osTypeHandlerSwitch = false
  var deviceTypeHandlerSwitch = false
  var ideaLengthHandlerSwitch = false
  var ideaIndustryAndSubHandlerSwitch = false
  var prefAdIndustryMatchedHandlerSwitch = false
  var userAdPrefHandlerSwitch = false
  var prefChannelMatchedHandlerSwitch = false
  var userChannelPrefHandlerSwitch = false
  var sexHandlerSwitch = false
  var sexIdeaIndustryAndSubHandlerSwitch = false
  var ageHandlerSwitch = false
  var ageIdeaIndustryAndSubHandlerSwitch = false
  var sexAgeHandlerSwitch = false
  var ideaHandlerSwitch = false
  var ideaVTRHandlerSwitch = false
  var adxPIdClientTypeHandlerSwitch = false
  var adxPIdClientTypeVTRHandlerSwitch = false
  var cityVTRHandlerSwitch = false
  var cityHandlerSwitch = false
  var videoIdHandlerSwitch = false
  var videoIdVTRHandlerSwitch = false
  var videoIdIdeaIdHandlerSwitch = false
  var videoIdIdeaIdVTRHandlerSwitch = false
  var subChannelHandlerSwitch = false
  var subChannelVTRHandlerSwitch = false
  var subChannelAdSubIndustryHandlerSwitch = false
  var subChannelAdSubIndustryVTRHandlerSwitch = false
  var channelAdIndustryHandlerSwitch = false
  var channelAdIndustryVTRHandlerSwitch = false
  var adxPIdSubChannelHandlerSwitch = false
  var adxPIdSubChannelVTRHandlerSwitch = false
  var adxPIdChannelHandlerSwitch = false
  var adxPIdChannelVTRHandlerSwitch = false
  var ideaLengthDis=15
  var VTRDiscreteDis = 0.05
  var VTRDiscreteLen = 20 //线性模型时，品牌广告里的VTR一般值为11，RTB广告里另取
  var continuousDiscrete = 1 //除VTR外的连续型值是否离散化，暂时不用

  def setParameters(props: Properties){
    timestampHandlerSwitch = props.getProperty("timestampHandlerSwitch", "0").toInt >0
    timeChannelHandlerSwitch = props.getProperty("timeChannelHandlerSwitch", "0").toInt >0
    timeClientTypeHandlerSwitch = props.getProperty("timeClientTypeHandlerSwitch", "0").toInt >0
    timeDeviceTypeHandlerSwitch = props.getProperty("timeDeviceTypeHandlerSwitch", "0").toInt >0
    timeOSTypeHandlerSwitch = props.getProperty("timeOSTypeHandlerSwitch", "0").toInt >0
    channelHandlerSwitch = props.getProperty("channelHandlerSwitch", "0").toInt >0
    osTypeHandlerSwitch = props.getProperty("osTypeHandlerSwitch", "0").toInt >0
    deviceTypeHandlerSwitch = props.getProperty("deviceTypeHandlerSwitch", "0").toInt >0
    ideaLengthHandlerSwitch = props.getProperty("ideaLengthHandlerSwitch", "0").toInt >0
    ideaIndustryAndSubHandlerSwitch = props.getProperty("ideaIndustryAndSubHandlerSwitch", "0").toInt >0
    prefAdIndustryMatchedHandlerSwitch = props.getProperty("prefAdIndustryMatchedHandlerSwitch", "0").toInt >0
    userAdPrefHandlerSwitch = props.getProperty("userAdPrefHandlerSwitch", "0").toInt >0
    prefChannelMatchedHandlerSwitch = props.getProperty("prefChannelMatchedHandlerSwitch", "0").toInt >0
    userChannelPrefHandlerSwitch = props.getProperty("userChannelPrefHandlerSwitch", "0").toInt >0
    sexHandlerSwitch = props.getProperty("sexHandlerSwitch", "0").toInt >0
    sexIdeaIndustryAndSubHandlerSwitch = props.getProperty("sexIdeaIndustryAndSubHandlerSwitch", "0").toInt >0
    ageHandlerSwitch = props.getProperty("ageHandlerSwitch", "0").toInt >0
    ageIdeaIndustryAndSubHandlerSwitch = props.getProperty("ageIdeaIndustryAndSubHandlerSwitch", "0").toInt >0
    sexAgeHandlerSwitch = props.getProperty("sexAgeHandlerSwitch", "0").toInt >0
    ideaHandlerSwitch = props.getProperty("ideaHandlerSwitch", "0").toInt >0
    ideaVTRHandlerSwitch = props.getProperty("ideaVTRHandlerSwitch", "0").toInt >0
    adxPIdClientTypeHandlerSwitch = props.getProperty("adxPIdClientTypeHandlerSwitch", "0").toInt >0
    adxPIdClientTypeVTRHandlerSwitch = props.getProperty("adxPIdClientTypeVTRHandlerSwitch", "0").toInt >0
    cityVTRHandlerSwitch = props.getProperty("cityVTRHandlerSwitch", "0").toInt >0
    cityHandlerSwitch = props.getProperty("cityHandlerSwitch", "0").toInt >0
    videoIdHandlerSwitch = props.getProperty("videoIdHandlerSwitch", "0").toInt >0
    videoIdVTRHandlerSwitch = props.getProperty("videoIdVTRHandlerSwitch", "0").toInt >0
    videoIdIdeaIdHandlerSwitch = props.getProperty("videoIdIdeaIdHandlerSwitch", "0").toInt >0
    videoIdIdeaIdVTRHandlerSwitch = props.getProperty("videoIdIdeaIdVTRHandlerSwitch", "0").toInt >0
    subChannelHandlerSwitch = props.getProperty("subChannelHandlerSwitch", "0").toInt >0
    subChannelVTRHandlerSwitch = props.getProperty("subChannelVTRHandlerSwitch", "0").toInt >0
    subChannelAdSubIndustryHandlerSwitch = props.getProperty("subChannelAdSubIndustryHandlerSwitch", "0").toInt >0
    subChannelAdSubIndustryVTRHandlerSwitch = props.getProperty("subChannelAdSubIndustryVTRHandlerSwitch", "0").toInt >0
    channelAdIndustryHandlerSwitch = props.getProperty("channelAdIndustryHandlerSwitch", "0").toInt >0
    channelAdIndustryVTRHandlerSwitch = props.getProperty("channelAdIndustryVTRHandlerSwitch", "0").toInt >0
    adxPIdSubChannelHandlerSwitch = props.getProperty("adxPIdSubChannelHandlerSwitch", "0").toInt >0
    adxPIdSubChannelVTRHandlerSwitch = props.getProperty("adxPIdSubChannelVTRHandlerSwitch", "0").toInt >0
    adxPIdChannelHandlerSwitch = props.getProperty("adxPIdChannelHandlerSwitch", "0").toInt >0
    adxPIdChannelVTRHandlerSwitch = props.getProperty("adxPIdChannelVTRHandlerSwitch", "0").toInt >0

    ideaLengthDis = props.getProperty("ideaLengthDis","15").toInt
    VTRDiscreteDis = props.getProperty("VTRDiscreteDis","0.05").toFloat
    VTRDiscreteLen = props.getProperty("VTRDiscreteLen","20").toInt
    continuousDiscrete = props.getProperty("VTRDiscreteLen","1").toInt
  }



}
