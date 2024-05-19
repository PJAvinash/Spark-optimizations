
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,lit}
import scala.collection.mutable.Queue


def unionDFS(dfseq:Seq[DataFrame]):DataFrame = {
  val unionSchema = dfseq.map(t => t.schema.map(t => t)).reduce( _ ++ _)
  val getUnionSelectExpr = (df:DataFrame) => {unionSchema.map(t => if (df.columns.contains(t.name)) col(t.name) else lit(null).cast(t.dataType).as(t.name))}
  return dfseq.map( t => t.select(getUnionSelectExpr(t):_*)).foldLeft(spark.emptyDataFrame){(unionDF,t) => unionDF.union(t)}
}



/*
** Helper functions for recursive operations
*/
def unionDFSrecursive(dfseq:Seq[DataFrame],from:Int,to:Int):DataFrame = {
  if(to == from){
    return spark.emptyDataFrame
  }
  if(to == 1 + from) {
    return dfseq(from)
  }
  if(to == 2+from){
    return dfseq(from).union(dfseq(from+1))
  }
  val mid:Int = (from+to)/2
  return unionDFSrecursive(dfseq,from,mid).union(unionDFSrecursive(dfseq,mid,to))
}
/*
** optimized implementation using binary reduction leading tp intermediate query plan size O(N log N)
*/
def unionDFSOptimized(dfseq:Seq[DataFrame]):DataFrame = {
  val unionSchema = dfseq.map(t => t.schema.map(t => t)).reduce( _ ++ _)
  val getUnionSelectExpr = (df:DataFrame) => {unionSchema.map(t => if (df.columns.contains(t.name)) col(t.name) else lit(null).cast(t.dataType).as(t.name))}
  return unionDFSrecursive(dfseq.map( t => t.select(getUnionSelectExpr(t):_*)),0,dfseq.size)
}

/*
** The following code uses BFS search like merge operations on dataframe unions reducing the intermediate query plan to a size O(N log_2 N)
** where N is #of dataframes 
*/
def unionDFSOptimized2(dfseq:Seq[DataFrame]):DataFrame = {
  if(dfseq.size == 0) {return spark.emptyDataFrame}
  val unionSchema = dfseq.map(t => t.schema.map(t => t)).reduce( _ ++ _)
  val getUnionSelectExpr = (df:DataFrame) => {unionSchema.map(t => if (df.columns.contains(t.name)) col(t.name) else lit(null).cast(t.dataType).as(t.name))}
  var dfQueue = Queue(dfseq.map( t => t.select(getUnionSelectExpr(t):_*)):_*)
  while(dfQueue.size > 1){
    // here were used reduction on 2 dataframes 
    val first = dfQueue.dequeue
    val second = dfQueue.dequeue
    dfQueue.enqueue(first.union(second))
  }
  return dfQueue(0)
}

/*
** The following code uses same idea as above on dataframe unions reducing the intermediate query plan to a size O(N log_4 N)
** where N is #of dataframes and has slightly better performance (not noticeable for most usecases)
*/
def unionDFSOptimized3(dfseq:Seq[DataFrame]):DataFrame = {
  if(dfseq.size == 0) {return spark.emptyDataFrame}
  val unionSchema = dfseq.map(t => t.schema.map(t => t)).reduce( _ ++ _)
  val getUnionSelectExpr = (df:DataFrame) => {unionSchema.map(t => if (df.columns.contains(t.name)) col(t.name) else lit(null).cast(t.dataType).as(t.name))}
  var dfQueue = Queue(dfseq.map( t => t.select(getUnionSelectExpr(t):_*)):_*)
  while(dfQueue.size > 3){
    // here were used reduction on 2 dataframes 
    val first = dfQueue.dequeue
    val second = dfQueue.dequeue
    val third = dfQueue.dequeue
    val fourth = dfQueue.dequeue
    dfQueue.enqueue(first.union(second).union(third.union(fourth)))
  }
   while(dfQueue.size > 1){
    // here were used reduction on 2 dataframes 
    val first = dfQueue.dequeue
    val second = dfQueue.dequeue
    dfQueue.enqueue(first.union(second))
  }
  return dfQueue(0)
}


val testSeq = Range(0,1000,10).map( t => spark.range(1*t,100*t).toDF)

/*
** optimized version call
*/
unionDFSOptimized3(testSeq).rdd.count

/*
** the following might fail with driver node failure.
*/
unionDFS(testSeq).rdd.count