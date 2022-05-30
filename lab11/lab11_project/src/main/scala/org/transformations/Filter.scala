package org.transformations

import org.case_classes.Flight

class Filter  extends java.io.Serializable{
  def countGt(flight_row: Flight, value: BigInt): Boolean = {

    return flight_row.count > value;
  }
}
