package application.advtr.transformer

import application.advtr.data.VTRDataInstance
import commons.framework.data.DataInstance
import commons.framework.transform.SparkTransform
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

/**
  * Created by Administrator on 2016/12/8.
  */
class PlainText2DataInstanceTransformer(val transformName: String = "PlainText2DataInstanceTransformer")
  extends SparkTransform{

  override var transformParams: Any = _

  type IN = String
  type OUT = DataInstance
  override implicit def tag: ClassTag[OUT] = PlainText2DataInstanceTransformer.tagDataInstance
  override var inputRDD: RDD[IN] = _
  override var outputRDD: RDD[OUT] = _

  /**
    * RTB广告VTR预估的格式
    * label,adPosition,timestamp,provinceId,cityId,retargetingId,videoId,ownerId,programeId,albumId,
         categoryId,SubCategoryId,keywords,clientType,osType,deviceType,domain,brower,appVersion,SiteId,
         adxPId(tagId),castId,costType,advertisterId,accountId,ideaId,category,subIndustry,industry,ideaLength,
         pageId,ideaSize,ideaType,      adsPref,channelPref,sex,age,cookie

   timestamp，provinceId，cityId，retargetingId，categoryId，SubCategoryId，clientType，osType，
   deviceType，brower，appVersion，SiteId，adxPId(tagId)，castId，advertisterId，ideaId，
   ideaLength，ideaSize，ideaType，             gender，age，adtags，channeltags
    */
  override def createTransformFunc(params: Any): String => DataInstance = {
    def transform(inputStr: String): DataInstance = {
      val inputs = inputStr.split("\t")
      //fill field
      val VTRDataInstance = new VTRDataInstance
      val label = inputs(0).toDouble
      VTRDataInstance.correctlyFilled = if(label != Double.MinValue && inputs.length >= 24)true else false
      var ideaLength = -1.0f
      var timeStamp: Long = -1
      try {
        ideaLength = inputs(5).toFloat
        timeStamp = inputs(6).toLong
      }catch {
        case ex: Exception => System.out.println("one dirty data,throw it!")
          VTRDataInstance.correctlyFilled = false;
      }
      if(VTRDataInstance.correctlyFilled) {

        VTRDataInstance.targetValue = label

        VTRDataInstance.adxPId = inputs(1)
        VTRDataInstance.clientType = inputs(2)
        VTRDataInstance.deviceType = inputs(3)
        VTRDataInstance.ideaId = inputs(4)
        VTRDataInstance.ideaLength = ideaLength
        VTRDataInstance.timeStamp = timeStamp
        VTRDataInstance.channel = inputs(7)
        VTRDataInstance.subChannel = inputs(8)
        VTRDataInstance.cityId = inputs(9)
        VTRDataInstance.osType = inputs(10)
        VTRDataInstance.advertiserId = inputs(11)
        VTRDataInstance.adCategory = inputs(12)
        VTRDataInstance.adSubIndustry = inputs(13)
        VTRDataInstance.adIndustry = inputs(14)

        VTRDataInstance.cookie = inputs(15)
        VTRDataInstance.castId = inputs(16)
        VTRDataInstance.videoId = inputs(17)
        VTRDataInstance.keywords = inputs(18)

        VTRDataInstance.userPlatform = inputs(19)
        VTRDataInstance.userAdIndustrys = inputs(20)
        VTRDataInstance.userChannels = inputs(21)
        VTRDataInstance.userSex = inputs(22)
        VTRDataInstance.userAge = inputs(23)

      }
      VTRDataInstance
    }
    transform
  }

  override def dataFilter(input: DataInstance): Boolean = {
    val inputIns = input.asInstanceOf[VTRDataInstance]
    inputIns.correctlyFilled
  }
}

object PlainText2DataInstanceTransformer{
  val tagDataInstance = reflect.classTag[DataInstance]
}
