package com.esri

import java.sql.Timestamp

case class TripOut(
                    pdate: Timestamp,
                    ddate: Timestamp,
                    passcount: Int,
                    triptime: Int,
                    tripdist: Double,
                    ploc: String,
                    dloc: String,
                    px: Double,
                    py: Double,
                    dx: Double,
                    dy: Double,
                    p100: String,
                    d100: String
                  )
