# 算子：localCheckpoint

## 1、源码

RDD.scala

```java
 /**
   * 使用Spark已存在的缓存层，标记这个RDD进行本地的checkpoint
   *
   * Mark this RDD for local checkpointing using Spark's existing caching layer.
   *
   * `这种方法适用于希望截断RDD血统，又不希望在可靠的分布式文件系统中复制物化数据这一昂贵步骤的用户`。
   *
   * `这对于具有需要定期截断的长血统的RDDs非常有用`。
   *
   * This method is for users who wish to truncate RDD lineages while skipping the expensive
   * step of replicating the materialized data in a reliable distributed file system. This is
   * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
   *
   * 本地checkpoint牺牲了容错能力。
   * 特别是，checkpoint数据被写入executors中的临时本地存储，而不是写入可靠的、容错的存储。
   * 其结果是，如果executors在计算期间失败，则checkpoint数据可能不再可访问，从而导致不可恢复的作业失败。
   *
   * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
   * data is written to ephemeral local storage in the executors instead of to a reliable,
   * fault-tolerant storage. The effect is that if an executor fails during the computation,
   * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
   *
   * 这与动态分配一起使用是不安全的，因为动态分配会删除executors及其缓存块。
   * 如果同时使用这两个属性，
   *   可以设置`spark.dynamicAllocation.cachedExecutorIdleTimeout`为一个大值。
   *
   * 通过`SparkContext#setCheckpointDir`设置的checkpoint目录将不再使用。
   *
   *【对于启用资源动态分配的情况下，executor是会随着任务进行动态分配与释放的，这便有对应的active状态与dead状态，对于dead之后executor的存储是无法访问了的，又重新分配的executor计算资源时也不一定能读到了】
   *
   * This is NOT safe to use with dynamic allocation, which removes executors along
   * with their cached blocks. If you must use both features, you are advised to set
   * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
   *
   * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
   */
  def localCheckpoint(): this.type = RDDCheckpointData.synchronized {
    if (conf.get(DYN_ALLOCATION_ENABLED) &&  //默认false
    	//默认Integer.MAX_VALUE
        conf.contains(DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT)) {
      logWarning("Local checkpointing is NOT safe to use with dynamic allocation, " +
        "which removes executors along with their cached blocks. If you must use both " +
        "features, you are advised to set `spark.dynamicAllocation.cachedExecutorIdleTimeout` " +
        "to a high value. E.g. If you plan to use the RDD for 1 hour, set the timeout to " +
        "at least 1 hour.")
    }

    // Note: At this point we do not actually know whether the user will call persist() on
    // this RDD later, so we must explicitly call it here ourselves to ensure the cached
    // blocks are registered for cleanup later in the SparkContext.
    //
    // If, however, the user has already called persist() on this RDD, then we must adapt
    // the storage level he/she specified to one that is appropriate for local checkpointing
    // (i.e. uses disk) to guarantee correctness.

//此时，并不知道用户之后是否会在这个RDD上调用persist()，所以我们必须在这就调用它，确保注册缓存块，为了后面的清理。
//如果用户已经调用了persist()，我们必须将用户指定的存储级别调整为适合本地checkpoint(即，使用磁盘)的存储级别，以确保正确性。
    if (storageLevel == StorageLevel.NONE) {
      persist(LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
    } else {
      persist(LocalRDDCheckpointData.transformStorageLevel(storageLevel), allowOverride = true)
    }

    // If this RDD is already checkpointed and materialized, its lineage is already truncated.
    // We must not override our `checkpointData` in this case because it is needed to recover
    // the checkpointed data. If it is overridden, next time materializing on this RDD will
    // cause error.
//如果这个RDD已经checkpointed and materialized，它的血统就早已截断，
//在这种情况下，我们不用覆盖这个`checkpointData`，【即不用再checkpoint、物化】
//因为需要使用它来恢复checkpointed数据。
//如果被覆盖了，下次在这个RDD上进行物化，就会报错。
    if (isCheckpointedAndMaterialized) {
      logWarning("Not marking RDD for local checkpoint because it was already " +
        "checkpointed and materialized")
    } else {
      // Lineage is not truncated yet, so just override any existing checkpoint data with ours
    //如果还没截断，那就要覆盖已存在的checkpoint数据
      checkpointData match {
        case Some(_: ReliableRDDCheckpointData[_]) => logWarning(
          "RDD was already marked for reliable checkpointing: overriding with local checkpoint.")
        case _ =>
      }
      checkpointData = Some(new LocalRDDCheckpointData(this))
    }
    this
  }
```

## 2、实例

```java
package func

import org.apache.spark.{SparkConf, SparkContext}

object localcheckpoint {
  def main(Args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("localcheckpoint").setMaster("local")
    val sc = new SparkContext(conf)
    
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val rdd2 = rdd1.map(x => x + 1).cache()

    rdd2.localCheckpoint()
    rdd2.collect().foreach(println)

//    rdd2.unpersist()
//    rdd2.collect().foreach(println)
    //抛异常：
    //Caused by: org.apache.spark.SparkException:
    // Checkpoint block rdd_1_0 not found!
    // Either the executor that originally checkpointed this partition is no longer alive,
    // or the original RDD is unpersisted.
    // If this problem persists, you may consider using `rdd.checkpoint()` instead,
    // which is slower than local checkpointing but more fault-tolerant.

    //rdd2在执行完localCheckpoint、collect后rdd2对rdd1的依赖切断并做了缓存，所以第一个collect指向成功。
    //随着rdd2.unpersist的调用，executor上的数据被清理，且此时切断了rdd1的依赖，所以再第二个collect抛异常
  }
}

```

参考：[https://zhuanlan.zhihu.com/p/87983748](https://zhuanlan.zhihu.com/p/87983748)