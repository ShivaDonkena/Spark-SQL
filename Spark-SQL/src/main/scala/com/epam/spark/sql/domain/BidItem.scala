package com.core.spark.sql.domain

/**
  * Created by Shiva_Donkena on 7/11/2018.
  */
case class BidItem (motelId:String,bidDate: String, loSa: String, price: String){

override def toString: String = s"$motelId,$bidDate,$loSa,$price"

}
