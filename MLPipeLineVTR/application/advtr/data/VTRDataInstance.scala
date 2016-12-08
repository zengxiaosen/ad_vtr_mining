package application.advtr.data

import commons.framework.data.DataInstance

/**
  * Created by Administrator on 2016/12/8.
  */
class VTRDataInstance extends DataInstance{
  override var correctlyFilled: Boolean = false
  var targetValue: Double = Double.MinValue

  var adxPId: String = null//1竞价广告位id
  var clientType: String = null//2web or app
  var deviceType: String = null//3
  var ideaId: String = null//素材id
  var ideaLength: Float = -1//5
  var timeStamp: Long = -1 //6
  var channel: String = null // 7
  var subChannel:String = null //8
  var cityId: String = null //9
  var osType: String = null //10
  var advertiserId:String = null //11
  var adCategory:String = null //12
  var adSubIndustry:String =null //13
  var adIndustry: String = null //14

  var cookie:String =null //15
  var castId:String =null //16
  var videoId:String =null //17
  var keywords:String =null //18

  var userPlatform:String=null //19
  var userAdIndustrys:String=null //20
  var userChannels:String=null //21
  var userSex:String=null //22
  var userAge:String=null //23
}
