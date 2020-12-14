# Dependency

```java
package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteProcessor}

/**
 * :: DeveloperApi ::
 * Base class for dependencies.  依赖的基类
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 * 
 * 子RDD的每个分区依赖于父RDD的少量的分区。窄依赖允许管道执行。
 *
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * 从子分区获取依赖的父分区
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * 关于一个 shuffle stage 输出的依赖
 *
 * 在 shuffle 中，因为在 executor 端不需要这个rdd，所以这个rdd是不可序列化的
 *
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. 
 * Note that in the case of shuffle, the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD  父RDD
 * @param partitioner partitioner used to partition the shuffle output 用来分区shuffle输出的分区器
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If not set
 *   explicitly then the default serializer, as specified by `spark.serializer` config option, will be used. 如果没有设置使用的序列化器，就使用`spark.serializer`指定的默认序列化器
 * @param keyOrdering key ordering for RDD's shuffles
 * @param aggregator map/reduce-side aggregator for RDD's shuffle  map/reduce端的聚合器
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine) 是否执行map端聚合
 * @param shuffleWriterProcessor the processor to control the write behavior in ShuffleMapTask 控制ShuffleMapTask中的写行为的处理器。
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],  //父RDD
    val partitioner: Partitioner,  //用来划分 shuffle 输出的分区器
    val serializer: Serializer = SparkEnv.get.serializer,
    val keyOrdering: Option[Ordering[K]] = None, // RDD's shuffles中，key 的排序方式
    val aggregator: Option[Aggregator[K, V, C]] = None, //map/reduce-side 的聚合器
    val mapSideCombine: Boolean = false, //是否执行map端聚合
    val shuffleWriterProcessor: ShuffleWriteProcessor = new ShuffleWriteProcessor)
  extends Dependency[Product2[K, V]] {

//如果要执行map端聚合，需要定义aggregator
  if (mapSideCombine) {
    require(aggregator.isDefined, "Map-side combine without Aggregator specified!")
  }

  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  //获得key\value的运行时类
  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  //注意：如果使用 PairRDDFunctions 中的 combineByKey 方法，而不使用 combineByKeyWithClassTag
  // combiner class tag 为空是可能的

  //获得combiner的运行时类
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  //为一次shuffle，创建一个id标识
  val shuffleId: Int = _rdd.context.newShuffleId()
  //  private val nextShuffleId = new AtomicInteger(0)
  // private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()


  // driver 使用 shuffleManager 注册 shuffles
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, this)

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
  _rdd.sparkContext.shuffleDriverComponents.registerShuffle(shuffleId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 * 窄依赖的子类：展示 子RDD分区 和 父RDD分区 的一对一的依赖
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 *
 * 窄依赖的子类：展示 子RDD分区 和 父RDD的一定范围内分区 的一对一的依赖 
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}

```