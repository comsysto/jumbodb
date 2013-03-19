package models

/**
 * User: carsten
 * Date: 3/19/13
 * Time: 1:47 PM
 */
case class ServerInformation(
  queryPort: Int,
  importPort: Int,
  dataPath: String,
  indexPath: String,
  maximumMemory: String,
  allocatedMemory: String,
  freeMemory: String,
  totalFreeMemory: String
)
