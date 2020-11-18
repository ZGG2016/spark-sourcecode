# 算子：checkpoint

## 1、源码

PairRDDFunctions.scala

```java
  /**
   * 标记这个RDD做checkpoint。
   * 将会存储到`SparkContext#setCheckpointDir`指定的目录下的文件中。
   * 所有对它的父RDD的引用都会被移除。
   * 必须在这个RDD上执行任何job之前调用此函数。
   * 建议将这个RDD持久化到内存，否则，将其保存到文件中将需要重新计算。
   *
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir` and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
   */
  def checkpoint(): Unit = RDDCheckpointData.synchronized {
  	//注意:为了确保子RDD分区指向正确的父分区，在这里使用了一个全局锁。今后我们应重新考虑这一问题。
    // NOTE: we use a global lock here due to complexities downstream with ensuring
    // children RDD partitions point to the correct parent partitions. In the future
    // we should revisit this consideration.
    //检查是否设置了checkpoint目录
    if (context.checkpointDir.isEmpty) {
      throw new SparkException("Checkpoint directory has not been set in the SparkContext")
    } else if (checkpointData.isEmpty) {
    	//将RDD数据写入到可靠存储的checkpoint的实现
      checkpointData = Some(new ReliableRDDCheckpointData(this))
    }
  }
```

## 2、示例

```java
import org.apache.spark.{SparkConf, SparkContext}

object CheckpointTest {
  def main(Args:Array[String]):Unit = {

    val conf = new SparkConf().setAppName("checkpoint").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("src/main/resources/checkpoint/")

    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9))
    val res = rdd.map(x=>x+1).cache()
    res.checkpoint()
    res.collect().foreach(println)

  }
}

//输出
/**
 * 目录: C:\Users\zgg\Desktop\ScalaCode\src\main\resources\checkpoint\975336d8-f3c2-4e7b-a14a-585e3fd20d9d\rdd-1
 *
 *
 * Mode          LastWriteTime                   Length   Name
 * ----          -------------                   ------   ----
 * -a----       2020/11/18     23:19             12      .part-00000.crc
 * -a----       2020/11/18     23:19            161       part-00000
 */
```