package com.hik.duplicateRemoval

import java.text.SimpleDateFormat
import java.util.Calendar
import com.hiklife.utils.{ByteUtil, HBaseUtil, RedisUtil}
import net.sf.json.{JSONArray, JSONObject}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._

object App {

  val MTCollectionAll = "MTCollectionAll"
  val MacRecoder = "MacRecoder"
  val MacRecoder_dateInx = "MacRecoder_dateInx"
  val IDRecoder = "MacRecoder"
  val IDRecoder_dateInx = "MacRecoder_dateInx"
  val APRecoder = "MacRecoder"
  val APRecoder_dateInx = "MacRecoder_dateInx"
  val MTMACInfo = "MTMACInfo"
  val MTIDInfo = "MTIDInfo"
  val MTAPInfo = "MTAPInfo"
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val conf = new SparkConf().setAppName("AppDuplicataRemvalJober")
    //conf.setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    //println(path+":path")
    val g2016conf = new ConfigUtil(path + "dataAnalysis/redis.xml")
    g2016conf.setConfPath(path)
    val HBASE_SITE_PATH=path + "hbase/hbase-site.xml"
    val hBaseUtil = new HBaseUtil(HBASE_SITE_PATH)
    hBaseUtil.createTable(MTCollectionAll, "CST")

    val broadList = sc.broadcast(List(HBASE_SITE_PATH, MacRecoder, IDRecoder, APRecoder))

    //统计单日
    val MacData = query("mac",HBASE_SITE_PATH, MacRecoder_dateInx, spark.sparkContext)
    val IDData = query("id",HBASE_SITE_PATH, IDRecoder_dateInx, spark.sparkContext)
    val APData = query("ap",HBASE_SITE_PATH, APRecoder_dateInx, spark.sparkContext)

    //统计全部
    val MTIDInfoCount = getTotalCount(HBASE_SITE_PATH, MTIDInfo)
    val MTMACInfoCount = getTotalCount(HBASE_SITE_PATH, MTMACInfo)
    val MTAPInfoCount = getTotalCount(HBASE_SITE_PATH, MTAPInfo)

    val mtJson = new JSONObject();
    mtJson.accumulate("mac", MTMACInfoCount)
    mtJson.accumulate("id", MTIDInfoCount)
    mtJson.accumulate("ap", MTAPInfoCount)

    val macCount = getMacCountByDays(MacData, broadList)
    val idCount = getIDCountByDays(IDData, broadList)
    val apCount = getAPCountByDays(APData, broadList)

    val mtCount = new JSONObject();
    mtCount.accumulate("mac", macCount)
    mtCount.accumulate("id", idCount)
    mtCount.accumulate("ap", apCount)
    val redisUtil = new RedisUtil(g2016conf.redisHost, g2016conf.redisPort, g2016conf.redisTimeout)
    redisUtil.setValue("DUV", mtCount.toString)
    redisUtil.setValue("DUTV", mtJson.toString)
  }


  def getTotalCount(path: String, tableName: String): Long = {
    val ac = new AggregationClient(HBaseUtil.getConfiguration(path))
    val scan = new Scan();
    scan.addFamily("S".getBytes);
    var rowCount = 0l
    rowCount = rowCount + ac.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan)
    rowCount
  }

  def getMacCountByDays(MacData: RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)], broadList: Broadcast[List[String]]): Long = {
    MacData.flatMap(x => {
      x._2.listCells()
    }).map(x => {
      val value=Bytes.toString(CellUtil.cloneValue(x)).replace("\n", "").replace("\r", "")
      value
    }).mapPartitions(partiton=>{
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val macRecorderTable = conn.getTable(TableName.valueOf(broadList.value(1).asInstanceOf[String])).asInstanceOf[HTable]
      macRecorderTable.setAutoFlush(false, false)
      macRecorderTable.setWriteBufferSize(5 * 1024 * 1024)
      val formatStr=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      partiton.map(line=>{
        val g:Get=new Get(line.getBytes)
        val r:Result=macRecorderTable.get(g)
        val b=r.getValue("RD".getBytes,"IN".getBytes)
        val j:JSONObject= JSONObject.fromObject(new String(b))
        val mac=j.get("mac").toString.replace("-","")
        val ct=j.get("ct").toString
        val timeDate=ct.substring(0,10)
        (mac,timeDate)
      })
    }).groupByKey().map(x=>{
      (x._1,1)
    }).count()
  }


  def getIDCountByDays(MacData:RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)],broadList:Broadcast[List[String]]):Long={
    MacData.flatMap(x=>{
      x._2.listCells()
    }).map(x=>{
      val value=Bytes.toString(CellUtil.cloneValue(x)).replace("\n","").replace("\r","")
      value
    }).mapPartitions(partiton=>{
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val idRecorderTable = conn.getTable(TableName.valueOf(broadList.value(2).asInstanceOf[String])).asInstanceOf[HTable]
      idRecorderTable.setAutoFlush(false, false)
      idRecorderTable.setWriteBufferSize(5 * 1024 * 1024)
      val formatStr=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      partiton.map(line=>{
        val g:Get=new Get(line.getBytes)
        val r:Result=idRecorderTable.get(g)
        val b=r.getValue("RD".getBytes,"IN".getBytes)
        val j:JSONObject= JSONObject.fromObject(new String(b))
        val id=j.get("ty")+"_"+j.get("id")
        val ct=j.get("ct").toString
        val timeDate=ct.substring(0,10)
        (id,timeDate)
      })
    }).groupByKey().map(x=>{
      (x._1,1)
    }).count()
  }

  def getAPCountByDays(APData:RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)],broadList:Broadcast[List[String]]):Long={
    APData.flatMap(x=>{
      x._2.listCells()
    }).map(x=>{
      val value=Bytes.toString(CellUtil.cloneValue(x)).replace("\n","").replace("\r","")
      value
    }).mapPartitions(partiton=>{
      val conn = ConnectionFactory.createConnection(HBaseUtil.getConfiguration(broadList.value(0).asInstanceOf[String]))
      val macRecorderTable = conn.getTable(TableName.valueOf(broadList.value(3).asInstanceOf[String])).asInstanceOf[HTable]
      macRecorderTable.setAutoFlush(false, false)
      macRecorderTable.setWriteBufferSize(5 * 1024 * 1024)
      val formatStr=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      partiton.map(line=>{
        val g:Get=new Get(line.getBytes)
        val r:Result=macRecorderTable.get(g)
        val b=r.getValue("RD".getBytes,"IN".getBytes)
        val j:JSONObject= JSONObject.fromObject(new String(b))
        val mac=j.get("mac").toString.replace("-","")
        val ct=j.get("ct").toString
        val timeDate=ct.substring(0,10)
        (mac,timeDate)
      })
    }).groupByKey().map(x=>{
      (x._1,1)
    }).count()

 /*     val mac=value.substring(0,12)
      (mac,1)
    }).groupByKey().map(x=>{
      (x._1,1)
    }).count()*/
  }
  /**
    * 生成HBASE RDD
    * @param epc
    * @param hbasepath
    * @param tableName
    * @param sc
    * @return
    */
  def query(types:String,hbasepath: String, tableName: String, sc: SparkContext): RDD[(org.apache.hadoop.hbase.io.ImmutableBytesWritable, org.apache.hadoop.hbase.client.Result)] ={
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("RD"))
    scan.addColumn(Bytes.toBytes("RD"),Bytes.toBytes("IN"))
    val mTime:Calendar=Calendar.getInstance()
    mTime.add(Calendar.DATE,-1)
    mTime.set(Calendar.HOUR,23)
    mTime.set(Calendar.MINUTE,59)
    mTime.set(Calendar.SECOND,59)

    val formatStr:SimpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val rowkey=getPrex(formatStr.format(mTime.getTime))
    val startrowkey=rowkey+getPrexTime(formatStr.format(mTime.getTime))
    scan.setStartRow(startrowkey.getBytes)
    mTime.add(Calendar.DATE,-1)
    val stoprowkey=rowkey+getPrexTime(formatStr.format(mTime.getTime))
    scan.setStopRow(stoprowkey.getBytes)
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray)
    val hconf = HBaseConfiguration.create()
    hconf.addResource(new Path(hbasepath))
    hconf.set(TableInputFormat.INPUT_TABLE, tableName)
    hconf.set(TableInputFormat.SCAN, ScanToString)
    val hBaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hBaseRDD
  }

  def getPrex(datetime:String):String={
    val dateTimes=datetime.substring(0,10).replace("-","")
    CommFunUtils.byte2HexStr(CommFunUtils.GetHashCodeWithLimit(dateTimes, 0xFF).toByte)
  }

  def getPrexTime(datetime:String):String={
    val bb = new Array[Byte](4)
    ByteUtil.putInt(bb, ("2524608000".toLong - CommFunUtils.Str2Date(datetime).getTime / 1000).asInstanceOf[Int], 0)
    CommFunUtils.byte2HexStr(bb)
  }

  def getPrexRowkey(datetime:String):String ={
    val dateTimes=datetime.substring(0,10).replace("-","")
    var keyrow = CommFunUtils.byte2HexStr(CommFunUtils.GetHashCodeWithLimit(dateTimes, 0xFF).toByte)
    val bb = new Array[Byte](4)
    ByteUtil.putInt(bb, ("2524608000".toLong - CommFunUtils.Str2Date(datetime).getTime / 1000).asInstanceOf[Int], 0)
    keyrow += CommFunUtils.byte2HexStr(bb)
    keyrow
  }


  //手动添加HBASE统计协调器，暂时不用，启用系统级别的协调器
  private def setCoprocessor(path: String) = {
    val configuraion = HBaseUtil.getConfiguration(path)
    val hbaseAdmin = new HBaseAdmin(configuraion)

    /*    hbaseAdmin.disableTable(MTIDInfo)
        hbaseAdmin.disableTable(MTMACInfo)
        hbaseAdmin.disableTable(MTAPInfo)*/
    if(hbaseAdmin.isTableAvailable(TableName.valueOf(MTMACInfo))){
      println("MTMACInfo 已被激活")
      hbaseAdmin.disableTable(MTMACInfo)
    }else{
      println("MTMACInfo 未被激活")
    }

    if(hbaseAdmin.isTableAvailable(TableName.valueOf(MTIDInfo))){
      println("MTIDInfo 已被激活")
      hbaseAdmin.disableTable(MTIDInfo)
    }else{
      println("MTIDInfo 未被激活")
    }

    if(hbaseAdmin.isTableAvailable(TableName.valueOf(MTAPInfo))){
      println("MTAPInfo 已被激活")
      hbaseAdmin.disableTable(MTAPInfo)
    }else{
      println("MTAPInfo 未被激活")
    }


    val machtd = hbaseAdmin.getTableDescriptor(TableName.valueOf(MTMACInfo))
    val idhtd = hbaseAdmin.getTableDescriptor(TableName.valueOf(MTIDInfo))
    val aphtd = hbaseAdmin.getTableDescriptor(TableName.valueOf(MTAPInfo))
    if(!aphtd.hasCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")){
      aphtd.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
    }
    if(!idhtd.hasCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")){
      idhtd.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
    }
    if(!machtd.hasCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")){
      machtd.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
    }

    hbaseAdmin.modifyTable(TableName.valueOf(MTMACInfo), machtd)
    hbaseAdmin.modifyTable(TableName.valueOf(MTIDInfo), idhtd)
    hbaseAdmin.modifyTable(TableName.valueOf(MTAPInfo), aphtd)

    hbaseAdmin.enableTable(TableName.valueOf(MTMACInfo))
    hbaseAdmin.enableTable(TableName.valueOf(MTIDInfo))
    hbaseAdmin.enableTable(TableName.valueOf(MTAPInfo))

    hbaseAdmin.close()
  }
}
