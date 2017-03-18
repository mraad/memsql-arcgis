package com.esri

import java.sql.Timestamp

case class TripInp(medallion: String,
                   license: String,
                   vendor: String,
                   rate_cd: String,
                   flag: String,
                   pdate: Timestamp,
                   ddate: Timestamp,
                   passcount: Int,
                   triptime: Int,
                   tripdist: Double,
                   plon: Double,
                   plat: Double,
                   dlon: Double,
                   dlat: Double
                  )