# UnsafeShuffleWriter

```java
package org.apache.spark.shuffle.sort;

import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nullable;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

import scala.Option;
import scala.Product2;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.internal.config.package$;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.network.util.LimitedInputStream;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TimeTrackingOutputStream;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.util.Utils;

@Private
public class UnsafeShuffleWriter<K, V> extends ShuffleWriter<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeShuffleWriter.class);

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  @VisibleForTesting
  static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  private final BlockManager blockManager;
  private final TaskMemoryManager memoryManager;
  private final SerializerInstance serializer;
  private final Partitioner partitioner;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final ShuffleExecutorComponents shuffleExecutorComponents;
  private final int shuffleId;
  private final long mapId;
  private final TaskContext taskContext;
  private final SparkConf sparkConf;
  private final boolean transferToEnabled;
  private final int initialSortBufferSize;
  private final int inputBufferSizeInBytes;

  // 一个ShuffleMapTask返回给一个scheduler的结果。
  // 包括运行任务的块管理器地址以及每个reducer的输出大小，以便传递给reduce任务。
  @Nullable private MapStatus mapStatus;

  // ShuffleExternalSorter：
  // 排序和合并多个类型为(K,V)的键值对，产生类型为(K,C)的key-combiner对。
  // 先使用一个分区器初次把 keys 划分到分区中，
  // 然后在每个分区中使用自定义的 Comparator 对 key 排序[可选步骤]。
  @Nullable private ShuffleExternalSorter sorter;
  private long peakMemoryUsedBytes = 0;

  //ByteArrayOutputStream：将数据写入字节数组的输出流，随着数据的写入，buffer自动增长。

  /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
  //直接暴露buf
  private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
    MyByteArrayOutputStream(int size) { super(size); }
    public byte[] getBuf() { return buf; }

    //The buffer where data is stored. 存储数据的buffer
    //protected byte buf[];
  }

  private MyByteArrayOutputStream serBuffer;
  private SerializationStream serOutputStream;  //写序列化对象的流

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
    //判断是否处在一个停止的进程中。
  private boolean stopping = false;

  public UnsafeShuffleWriter(
      BlockManager blockManager,
      TaskMemoryManager memoryManager,  //管理一个独立任务分配的内存
      SerializedShuffleHandle<K, V> handle,//用来识别什么时候使用serialized shuffle
      long mapId,
      TaskContext taskContext,
      SparkConf sparkConf,
      ShuffleWriteMetricsReporter writeMetrics,//对每个shuffle，报告shuffle读取指标。
      ShuffleExecutorComponents shuffleExecutorComponents) {
    // 获取分区数
    final int numPartitions = handle.dependency().partitioner().numPartitions();
    //MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE：
    //当以序列化形式缓存 map 输出时，SortShuffleManager 支持的最大 shuffle 输出分区数。
    if (numPartitions > SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE()) {
      throw new IllegalArgumentException(
        "UnsafeShuffleWriter can only be used for shuffles with at most " +
        SortShuffleManager.MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE() +
        " reduce partitions");
    }
    this.blockManager = blockManager;
    this.memoryManager = memoryManager;
    this.mapId = mapId;
    final ShuffleDependency<K, V, V> dep = handle.dependency();
    this.shuffleId = dep.shuffleId();
    this.serializer = dep.serializer().newInstance();
    this.partitioner = dep.partitioner();
    this.writeMetrics = writeMetrics;
    this.shuffleExecutorComponents = shuffleExecutorComponents;
    this.taskContext = taskContext;
    this.sparkConf = sparkConf;
    this.transferToEnabled = sparkConf.getBoolean("spark.file.transferTo", true);
    this.initialSortBufferSize =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());
    this.inputBufferSizeInBytes =
      (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    open();
  }

  private void updatePeakMemoryUsed() {
    // sorter can be null if this writer is closed
    if (sorter != null) {
      //回到目前为止使用的峰值内存，以字节为单位。
      long mem = sorter.getPeakMemoryUsedBytes();
      if (mem > peakMemoryUsedBytes) {
        peakMemoryUsedBytes = mem;
      }
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  //回到目前为止使用的峰值内存，以字节为单位。
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * This convenience method should only be called in test code.
   */
  @VisibleForTesting
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    write(JavaConverters.asScalaIteratorConverter(records).asScala());
  }

  @Override
  public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
    // Keep track of success so we know if we encountered an exception
    // We do this rather than a standard try/catch/re-throw to handle
    // generic throwables.
    //追踪success，为了知道我们是否遇到了一个异常。
    boolean success = false;
    try {
      while (records.hasNext()) {  //还有记录就插进去
        insertRecordIntoSorter(records.next());
      }
      closeAndWriteOutput();
      success = true;
    } finally {
      if (sorter != null) {
        try {
          sorter.cleanupResources();
        } catch (Exception e) {
          // Only throw this error if we won't be masking another
          // error.
          if (success) {
            throw e;
          } else {
            logger.error("In addition to a failure during writing, we failed during " +
                         "cleanup.", e);
          }
        }
      }
    }
  }

  private void open() {
    assert (sorter == null);
    sorter = new ShuffleExternalSorter(
      memoryManager,
      blockManager,
      taskContext,
      initialSortBufferSize,
      partitioner.numPartitions(),
      sparkConf,
      writeMetrics);
    serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
    serOutputStream = serializer.serializeStream(serBuffer);
  }

  @VisibleForTesting
  void closeAndWriteOutput() throws IOException {
    assert(sorter != null);
    updatePeakMemoryUsed();
    serBuffer = null;
    serOutputStream = null;
    //closeAndGetSpills：关闭sorter，使所有缓冲的数据被排序，并写入磁盘。
    //返回这个sorter写的溢写文件的元数据。
    final SpillInfo[] spills = sorter.closeAndGetSpills();
    sorter = null;
    final long[] partitionLengths;
    try {
      //合并0个或多个溢写文件，返回合并的文件的分区长度的数组
      partitionLengths = mergeSpills(spills);
    } finally {
      for (SpillInfo spill : spills) {
        if (spill.file.exists() && !spill.file.delete()) {
          logger.error("Error while deleting spill file {}", spill.file.getPath());
        }
      }
    }
    mapStatus = MapStatus$.MODULE$.apply(
      blockManager.shuffleServerId(), partitionLengths, mapId);
  }

  @VisibleForTesting
  void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
    assert(sorter != null);
    final K key = record._1();
    final int partitionId = partitioner.getPartition(key);
    // reset：重置字节数组输出流的count字段为0
    // count：缓存中有效的字节数
    serBuffer.reset();
    serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
    serOutputStream.flush();

    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    //将一条记录写入到 shuffle sorter
    sorter.insertRecord(
      serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
  }

  @VisibleForTesting
  void forceSorterToSpill() throws IOException {
    assert (sorter != null);
    sorter.spill();
  }

-----------------------------------------------------

  /**
   * 合并0个或多个溢写文件，基于溢写的数量和IO压缩格式，选择最快的合并策略
   *
   * Merge zero or more spill files together, choosing the fastest merging strategy based on the
   * number of spills and the IO compression codec.
   *
   * @return the partition lengths in the merged file. 合并的文件的分区长度的数组
   */
  private long[] mergeSpills(SpillInfo[] spills) throws IOException {
    long[] partitionLengths;
    if (spills.length == 0) {
      final ShuffleMapOutputWriter mapWriter = shuffleExecutorComponents
          .createMapOutputWriter(shuffleId, mapId, partitioner.numPartitions());
      return mapWriter.commitAllPartitions();
    } else if (spills.length == 1) {
      Optional<SingleSpillShuffleMapOutputWriter> maybeSingleFileWriter =
          shuffleExecutorComponents.createSingleFileMapOutputWriter(shuffleId, mapId);
      if (maybeSingleFileWriter.isPresent()) {
        // Here, we don't need to perform any metrics updates because the bytes written to this
        // output file would have already been counted as shuffle bytes written.
        partitionLengths = spills[0].partitionLengths;
        logger.debug("Merge shuffle spills for mapId {} with length {}", mapId,
            partitionLengths.length);
        maybeSingleFileWriter.get().transferMapSpillFile(spills[0].file, partitionLengths);
      } else {
        partitionLengths = mergeSpillsUsingStandardWriter(spills);
      }
    } else {
      partitionLengths = mergeSpillsUsingStandardWriter(spills);
    }
    return partitionLengths;
  }

  private long[] mergeSpillsUsingStandardWriter(SpillInfo[] spills) throws IOException {
    long[] partitionLengths;
    final boolean compressionEnabled = (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_COMPRESS());
    final CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    final boolean fastMergeEnabled =
        (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_UNSAFE_FAST_MERGE_ENABLE());
    final boolean fastMergeIsSupported = !compressionEnabled ||
        CompressionCodec$.MODULE$.supportsConcatenationOfSerializedStreams(compressionCodec);
    final boolean encryptionEnabled = blockManager.serializerManager().encryptionEnabled();
    final ShuffleMapOutputWriter mapWriter = shuffleExecutorComponents
        .createMapOutputWriter(shuffleId, mapId, partitioner.numPartitions());
    try {
      // There are multiple spills to merge, so none of these spill files' lengths were counted
      // towards our shuffle write count or shuffle write time. If we use the slow merge path,
      // then the final output file's size won't necessarily be equal to the sum of the spill
      // files' sizes. To guard against this case, we look at the output file's actual size when
      // computing shuffle bytes written.
      //
      // We allow the individual merge methods to report their own IO times since different merge
      // strategies use different IO techniques.  We count IO during merge towards the shuffle
      // write time, which appears to be consistent with the "not bypassing merge-sort" branch in
      // ExternalSorter.
      if (fastMergeEnabled && fastMergeIsSupported) {
        // Compression is disabled or we are using an IO compression codec that supports
        // decompression of concatenated compressed streams, so we can perform a fast spill merge
        // that doesn't need to interpret the spilled bytes.
        if (transferToEnabled && !encryptionEnabled) {
          logger.debug("Using transferTo-based fast merge");
          mergeSpillsWithTransferTo(spills, mapWriter);
        } else {
          logger.debug("Using fileStream-based fast merge");
          mergeSpillsWithFileStream(spills, mapWriter, null);
        }
      } else {
        logger.debug("Using slow merge");
        mergeSpillsWithFileStream(spills, mapWriter, compressionCodec);
      }
      // When closing an UnsafeShuffleExternalSorter that has already spilled once but also has
      // in-memory records, we write out the in-memory records to a file but do not count that
      // final write as bytes spilled (instead, it's accounted as shuffle write). The merge needs
      // to be counted as shuffle write, but this will lead to double-counting of the final
      // SpillInfo's bytes.
      writeMetrics.decBytesWritten(spills[spills.length - 1].file.length());
      partitionLengths = mapWriter.commitAllPartitions();
    } catch (Exception e) {
      try {
        mapWriter.abort(e);
      } catch (Exception e2) {
        logger.warn("Failed to abort writing the map output.", e2);
        e.addSuppressed(e2);
      }
      throw e;
    }
    return partitionLengths;
  }

  /**
   * Merges spill files using Java FileStreams. This code path is typically slower than
   * the NIO-based merge, {@link UnsafeShuffleWriter#mergeSpillsWithTransferTo(SpillInfo[],
   * ShuffleMapOutputWriter)}, and it's mostly used in cases where the IO compression codec
   * does not support concatenation of compressed data, when encryption is enabled, or when
   * users have explicitly disabled use of {@code transferTo} in order to work around kernel bugs.
   * This code path might also be faster in cases where individual partition size in a spill
   * is small and UnsafeShuffleWriter#mergeSpillsWithTransferTo method performs many small
   * disk ios which is inefficient. In those case, Using large buffers for input and output
   * files helps reducing the number of disk ios, making the file merging faster.
   *
   * @param spills the spills to merge.
   * @param mapWriter the map output writer to use for output.
   * @param compressionCodec the IO compression codec, or null if shuffle compression is disabled.
   * @return the partition lengths in the merged file.
   */
  private void mergeSpillsWithFileStream(
      SpillInfo[] spills,
      ShuffleMapOutputWriter mapWriter,
      @Nullable CompressionCodec compressionCodec) throws IOException {
    logger.debug("Merge shuffle spills with FileStream for mapId {}", mapId);
    final int numPartitions = partitioner.numPartitions();
    final InputStream[] spillInputStreams = new InputStream[spills.length];

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputStreams[i] = new NioBufferedFileInputStream(
          spills[i].file,
          inputBufferSizeInBytes);
        // Only convert the partitionLengths when debug level is enabled.
        if (logger.isDebugEnabled()) {
          logger.debug("Partition lengths for mapId {} in Spill {}: {}", mapId, i,
              Arrays.toString(spills[i].partitionLengths));
        }
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        boolean copyThrewException = true;
        ShufflePartitionWriter writer = mapWriter.getPartitionWriter(partition);
        OutputStream partitionOutput = writer.openStream();
        try {
          partitionOutput = new TimeTrackingOutputStream(writeMetrics, partitionOutput);
          partitionOutput = blockManager.serializerManager().wrapForEncryption(partitionOutput);
          if (compressionCodec != null) {
            partitionOutput = compressionCodec.compressedOutputStream(partitionOutput);
          }
          for (int i = 0; i < spills.length; i++) {
            final long partitionLengthInSpill = spills[i].partitionLengths[partition];

            if (partitionLengthInSpill > 0) {
              InputStream partitionInputStream = null;
              boolean copySpillThrewException = true;
              try {
                partitionInputStream = new LimitedInputStream(spillInputStreams[i],
                    partitionLengthInSpill, false);
                partitionInputStream = blockManager.serializerManager().wrapForEncryption(
                    partitionInputStream);
                if (compressionCodec != null) {
                  partitionInputStream = compressionCodec.compressedInputStream(
                      partitionInputStream);
                }
                ByteStreams.copy(partitionInputStream, partitionOutput);
                copySpillThrewException = false;
              } finally {
                Closeables.close(partitionInputStream, copySpillThrewException);
              }
            }
          }
          copyThrewException = false;
        } finally {
          Closeables.close(partitionOutput, copyThrewException);
        }
        long numBytesWritten = writer.getNumBytesWritten();
        writeMetrics.incBytesWritten(numBytesWritten);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (InputStream stream : spillInputStreams) {
        Closeables.close(stream, threwException);
      }
    }
  }

  /**
   * Merges spill files by using NIO's transferTo to concatenate spill partitions' bytes.
   * This is only safe when the IO compression codec and serializer support concatenation of
   * serialized streams.
   *
   * @param spills the spills to merge.
   * @param mapWriter the map output writer to use for output.
   * @return the partition lengths in the merged file.
   */
  private void mergeSpillsWithTransferTo(
      SpillInfo[] spills,
      ShuffleMapOutputWriter mapWriter) throws IOException {
    logger.debug("Merge shuffle spills with TransferTo for mapId {}", mapId);
    final int numPartitions = partitioner.numPartitions();
    final FileChannel[] spillInputChannels = new FileChannel[spills.length];
    final long[] spillInputChannelPositions = new long[spills.length];

    boolean threwException = true;
    try {
      for (int i = 0; i < spills.length; i++) {
        spillInputChannels[i] = new FileInputStream(spills[i].file).getChannel();
        // Only convert the partitionLengths when debug level is enabled.
        if (logger.isDebugEnabled()) {
          logger.debug("Partition lengths for mapId {} in Spill {}: {}", mapId, i,
              Arrays.toString(spills[i].partitionLengths));
        }
      }
      for (int partition = 0; partition < numPartitions; partition++) {
        boolean copyThrewException = true;
        ShufflePartitionWriter writer = mapWriter.getPartitionWriter(partition);
        WritableByteChannelWrapper resolvedChannel = writer.openChannelWrapper()
            .orElseGet(() -> new StreamFallbackChannelWrapper(openStreamUnchecked(writer)));
        try {
          for (int i = 0; i < spills.length; i++) {
            long partitionLengthInSpill = spills[i].partitionLengths[partition];
            final FileChannel spillInputChannel = spillInputChannels[i];
            final long writeStartTime = System.nanoTime();
            Utils.copyFileStreamNIO(
                spillInputChannel,
                resolvedChannel.channel(),
                spillInputChannelPositions[i],
                partitionLengthInSpill);
            copyThrewException = false;
            spillInputChannelPositions[i] += partitionLengthInSpill;
            writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
          }
        } finally {
          Closeables.close(resolvedChannel, copyThrewException);
        }
        long numBytes = writer.getNumBytesWritten();
        writeMetrics.incBytesWritten(numBytes);
      }
      threwException = false;
    } finally {
      // To avoid masking exceptions that caused us to prematurely enter the finally block, only
      // throw exceptions during cleanup if threwException == false.
      for (int i = 0; i < spills.length; i++) {
        assert(spillInputChannelPositions[i] == spills[i].file.length());
        Closeables.close(spillInputChannels[i], threwException);
      }
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

      if (stopping) {
        return Option.apply(null);
      } else {
        stopping = true;
        if (success) {
          if (mapStatus == null) {
            throw new IllegalStateException("Cannot call stop(true) without having called write()");
          }
          return Option.apply(mapStatus);
        } else {
          return Option.apply(null);
        }
      }
    } finally {
      if (sorter != null) {
        // If sorter is non-null, then this implies that we called stop() in response to an error,
        // so we need to clean up memory and spill files created by the sorter
        sorter.cleanupResources();
      }
    }
  }

  private static OutputStream openStreamUnchecked(ShufflePartitionWriter writer) {
    try {
      return writer.openStream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final class StreamFallbackChannelWrapper implements WritableByteChannelWrapper {
    private final WritableByteChannel channel;

    StreamFallbackChannelWrapper(OutputStream fallbackStream) {
      this.channel = Channels.newChannel(fallbackStream);
    }

    @Override
    public WritableByteChannel channel() {
      return channel;
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }
}

```