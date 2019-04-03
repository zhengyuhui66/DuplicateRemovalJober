package com.hik.duplicateRemoval

import java.text.SimpleDateFormat
import java.util.{Date, GregorianCalendar}

import com.hiklife.utils.{ByteUtil, RedisUtil}
import org.apache.hadoop.hbase.client.{HTable, Put}

object CommFunUtils  extends Serializable{

  val ENTER:String="1"
  val EXIT:String="0"
  val AP="ap"
  val MAC="mac"
  val ID="id"
  val MINNAME="devmin"
  val SPLIT="_"

  def byte2HexStr(b:Byte):String={
    var hs=""
    var stmp=(b&0xFF).toHexString.toUpperCase
    hs=if(stmp.length==1){
      hs+"0"+stmp
    }else{
      hs+stmp
    }
    hs
  }


  def GetHashCodeWithLimit(context: String, limit: Int): Int =  {
    var hash = 0
    for (item <- context.getBytes)  {
      hash = 33 * hash + item
    }
    return (hash % limit)
  }


  def byte2HexStr(b: Array[Byte]): String =  { var hs: String = ""
    var stmp: String = ""
    var n: Int = 0
    for(i<-0 until b.length){
      stmp=(b(i)&0XFF).toHexString
      if(stmp.length==1){
        hs=hs+"0"+stmp
      }else{
        hs=hs+stmp
      }
    }
    return hs.toUpperCase
  }
  /**
    * 字符串(YYYY-MM-DD hh:mm:ss)转换成Date
    *
    * @param s
    * @return
    */
  def Str2Date(s: String): Date ={
    if (!(s == null || (s.equals("")))) try {
      val gc = new GregorianCalendar
      gc.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s))
      gc.getTime
    } catch {
      case e: Exception =>{
        print(e)
        null
      }
    }
    else null
  }
  //获取当前月时间
  def getMonthNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMM")
    s.format(new Date())
  }
  //获取当前天时间
  def getNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMMdd")
    s.format(new Date())
  }
  //根据采集分钟获取
  def getMinNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHHmm")
    s.format(new Date())
  }
  //获取小时时间
  def getHourNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHH")
    s.format(new Date())
  }



  /* /**
     * byte转换成十六进制字符串
     */
   public static String byte2HexStr(byte b) {
     String hs = "";
     String stmp = (Integer.toHexString(b & 0XFF));
     if (stmp.length() == 1)
       hs = hs + "0" + stmp;
     else
       hs = hs + stmp;

     return hs.toUpperCase();
   }


      /**
      * 获取指定范围内的简单散列值
      * @param context 要散列的内容
      * @param limit 散列范围
      * @return
      */def GetHashCodeWithLimit(context: String, limit: Int): Int =  { var hash: Int = 0
 for (item <- context.getBytes)  { hash = 33 * hash + item
 }
 return (hash % limit)
 }



    /**
      * bytes转换成十六进制字符串
      */def byte2HexStr(b: Array[Byte]): String =  { var hs: String = ""
 var stmp: String = ""
 var n: Int = 0
 while ( { n < b.length})  { stmp = (Integer.toHexString(b(n) & 0XFF))
 if (stmp.length == 1)  { hs = hs + "0" + stmp}
 else  { hs = hs + stmp}

 {n += 1; n - 1}}
 return hs.toUpperCase
 }
   */
}
