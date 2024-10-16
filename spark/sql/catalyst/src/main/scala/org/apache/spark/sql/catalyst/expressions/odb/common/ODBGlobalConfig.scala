package org.apache.spark.sql.catalyst.expressions.odb.common

import org.apache.spark.broadcast.Broadcast

class ODBGlobalConfig {
  var broadcastConfig: Broadcast[ODBConfigConstants.type] = _
}
