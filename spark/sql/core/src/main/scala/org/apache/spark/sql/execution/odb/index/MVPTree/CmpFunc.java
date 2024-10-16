package org.apache.spark.sql.execution.odb.index.MVPTree;

import org.apache.spark.sql.execution.odb.index.entity.MVPDP;

public interface CmpFunc {
    float compare(MVPDP pointA, MVPDP pointB);
}
