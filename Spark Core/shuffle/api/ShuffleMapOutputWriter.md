# ShuffleMapOutputWriter

```java
package org.apache.spark.shuffle.api;

import java.io.IOException;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * A top-level writer that returns child writers for persisting the output of a map task,
 * and then commits all of the writes as one atomic operation.
 *
 * @since 3.0.0
 */
@Private
public interface ShuffleMapOutputWriter {

  /**
   * Creates a writer that can open an output stream to persist bytes targeted for a given reduce
   * partition id.
   * <p>
   * The chunk corresponds to bytes in the given reduce partition. This will not be called twice
   * for the same partition within any given map task. The partition identifier will be in the
   * range of precisely 0 (inclusive) to numPartitions (exclusive), where numPartitions was
   * provided upon the creation of this map output writer via
   * {@link ShuffleExecutorComponents#createMapOutputWriter(int, long, int)}.
   * <p>
   * Calls to this method will be invoked with monotonically increasing reducePartitionIds; each
   * call to this method will be called with a reducePartitionId that is strictly greater than
   * the reducePartitionIds given to any previous call to this method. This method is not
   * guaranteed to be called for every partition id in the above described range. In particular,
   * no guarantees are made as to whether or not this method will be called for empty partitions.
   */
  ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException;

  /**
   * 提交所有 partition writers 完成的写入操作，返回每个分区写入的字节数。partition writers 是所有调用这个对象的 getPartitionWriter(int) 方法返回的。
   *
   * Commits the writes done by all partition writers returned by all calls to this object's
   * {@link #getPartitionWriter(int)}, and returns the number of bytes written for each
   * partition.
   *
   * 这应该确保这个模块的 partition writers 产生的 writes 对下游 reduce tasks 可用。
   * 如果这个方法抛出任意异常，就会调用这个模块的 abort(Throwable) 方法，在传播执行之前。
   *
   * 这也可以关闭资源，清理临时状态。
   *
   * <p>
   * This should ensure that the writes conducted by this module's partition writers are
   * available to downstream reduce tasks. If this method throws any exception, this module's
   * {@link #abort(Throwable)} method will be invoked before propagating the exception.
   * <p>
   * This can also close any resources and clean up temporary state if necessary.
   * <p>
   *
   * 返回的数组应该包含 partition writer 往这个分区id 写入的字节数，
   *                 分区是从0到(numPartitions - 1)
   *
   * The returned array should contain, for each partition from (0) to (numPartitions - 1), the
   * number of bytes written by the partition writer for that partition id.
   */
  long[] commitAllPartitions() throws IOException;

  /**
   * Abort all of the writes done by any writers returned by {@link #getPartitionWriter(int)}.
   * <p>
   * This should invalidate the results of writing bytes. This can also close any resources and
   * clean up temporary state if necessary.
   */
  void abort(Throwable error) throws IOException;
}

```