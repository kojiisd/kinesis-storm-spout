package com.amazonaws.services.kinesis.stormspout;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.exceptions.InvalidSeekPositionException;
import com.amazonaws.services.kinesis.stormspout.exceptions.KinesisSpoutException;
import com.google.common.collect.ImmutableList;

/**
 * @author yeminkhaung
 *
 */
public class KinesisTridentEmitter implements Emitter<Map<String, Object>> {

	/** Logger */
	private static final Logger logger = LoggerFactory
			.getLogger(KinesisTridentEmitter.class);

	/** Kinesis Spout Configuration */
	private KinesisSpoutConfig spoutConfig;

	/** Topology Context */
	private TopologyContext context;

	/** Specific Configurations */
	private Map<String, Object> conf;

	private InitialPositionInStream initialPosition;

	private IShardGetterBuilder getterBuilder;

	private IShardListGetter shardListGetter;

	private KinesisTridentStateManager stateManager;

	private Map<String, Object> messageList;

	private Map<String, String> shardList;

	boolean isNotPrepared = true;

	private long emptyRecordListSleepTimeMillis = 5L;

	private final int maxRecordSize = 1000000;

	/**
	 * @param config
	 * @param shardListGetter
	 * @param getterBuilder
	 * @param initialPosition
	 */
	public KinesisTridentEmitter(KinesisSpoutConfig config,
			IShardListGetter shardListGetter,
			IShardGetterBuilder getterBuilder,
			InitialPositionInStream initialPosition) {
		this.spoutConfig = config;
		this.shardListGetter = shardListGetter;
		this.getterBuilder = getterBuilder;
		this.initialPosition = initialPosition;
	}

	/**
	 * Initialize message configuration
	 * 
	 * @param txid
	 *            txid
	 * @param conf
	 *            configuration
	 * @param context
	 *            topology context
	 */
	public void init(String txid, Map<String, Object> conf,
			TopologyContext context) {
		logger.debug("Initializing Kinesis Trident Emitter. {}, {}, {}",
				new Object[] { txid, conf.toString(), context.toString() });
		try {
			this.context = context;
			this.conf = conf;
			messageList = new ConcurrentHashMap<String, Object>();
			shardList = new ConcurrentHashMap<String, String>();
		} catch (Exception ex) {
			logger.warn("Error initializing the Emitter.",
					new KinesisSpoutException(ex));
		}
	}

	@Override
	public void close() {
		try {
			logger.debug("Closing Kinesis Trident Emitter.");
			for (Entry<String, Object> message : this.messageList.entrySet()) {

				String shardId = KinesisTridentStateManager.splitMessageId(
						message.getKey(), ":",
						KinesisTridentStateManager.SHARD_SPLIT);

				stateManager.emit(shardId,
						(Record) messageList.get(message.getKey()), false);
			}

		} catch (Exception ex) {
			logger.debug("Cannot cleanup records.", ex);
		}
	}

	@Override
	public void emitBatch(TransactionAttempt tx,
			Map<String, Object> collectorMetadata, TridentCollector collector) {
		if (isNotPrepared) {
			prepare();
		}

		try {
			synchronized (stateManager) {
				if (!stateManager.hasGetters()) {
					try {
						Thread.sleep(emptyRecordListSleepTimeMillis);
					} catch (InterruptedException e) {
						logger.debug(this + " sleep was interrupted.");
					}
					return;
				}

				final IShardGetter getter = stateManager.getNextGetter();
				String currentMsgId = shardList
						.get(getter.getAssociatedShard());
				boolean isRetry = false;
				if (currentMsgId != null) {
					String lastReadSeqNum = KinesisTridentStateManager
							.splitMessageId(currentMsgId, ":",
									KinesisTridentStateManager.SEQ_SPLIT);
					// Seek to last read record's sequence
					try {
						logger.debug("Seek to Sequence Number: "
								+ lastReadSeqNum + " in the shard.");
						getter.seek(ShardPosition
								.afterSequenceNumber(lastReadSeqNum));
					} catch (InvalidSeekPositionException ex) {
						logger.warn("Error seeking to last read sequence");
					}
				}
				final ImmutableList<Record> records = getter.getNext(
						maxRecordSize).getRecords();

				if (records.size() != 0) {
					for (Record rec : records) {
						Record recordToEmit = copyRecord(rec);
						List<Object> tuple = spoutConfig.getScheme()
								.deserialize(recordToEmit);

						logger.info(
								this
										+ " emitting record with seqnum [{}] from shard [{}] from batch [{}].",
								new Object[] {
										recordToEmit.getSequenceNumber(),
										getter.getAssociatedShard(),
										recordToEmit.getPartitionKey() });
						collector.emit(new Values(tuple.toArray()));
						stateManager.emit(getter.getAssociatedShard(),
								recordToEmit, isRetry);
						currentMsgId = KinesisTridentStateManager
								.makeMessageId(recordToEmit.getPartitionKey(),
										getter.getAssociatedShard(),
										recordToEmit.getSequenceNumber());
						shardList
								.put(getter.getAssociatedShard(), currentMsgId);
						messageList.put(currentMsgId, recordToEmit);
					}

				} else {
					// Sleep here for a bit if there were no records to emit.
					logger.info("No record to emit.");
					try {
						Thread.sleep(emptyRecordListSleepTimeMillis);
					} catch (InterruptedException e) {
						logger.debug(this + " sleep was interrupted.");
					}
				}
				stateManager.commitShardStates();
			}
		} catch (Exception ex) {
			logger.warn("Error emitting batch records.", ex);
		}
	}

	@Override
	public void success(TransactionAttempt tx) {
		try {
			for (Entry<String, Object> message : this.messageList.entrySet()) {
				String msgId = message.getKey();
				String shardId = KinesisTridentStateManager.splitMessageId(
						msgId, ":", KinesisTridentStateManager.SHARD_SPLIT);

				String seqNum = KinesisTridentStateManager.splitMessageId(
						msgId, ":", KinesisTridentStateManager.SHARD_SPLIT);
				logger.info("Acking emitted records.");
				stateManager.ack(shardId, seqNum);
				messageList.remove(msgId);
			}
			logger.info("Saving shard states");
			stateManager.commitShardStates();
			logger.debug("Transaction:{} is succeeded.", tx);
		} catch (Exception ex) {
			logger.debug("Finishing up records failed.", ex);
		}

	}

	/**
	 * Prepare trident spout
	 * 
	 * @param collector
	 */
	private void prepare() {
		logger.info(
				"Preparing Kinesis Trident State Manager with {}, {}, {}, {}. ",
				new Object[] { spoutConfig.toString(),
						shardListGetter.toString(), getterBuilder.toString(),
						initialPosition });
		try {
			stateManager = new KinesisTridentStateManager(spoutConfig,
					shardListGetter, getterBuilder, initialPosition);
			final int taskIndex = context.getThisTaskIndex();
			final int totalNumTasks = context.getComponentTasks(
					context.getThisComponentId()).size();
			logger.info("Activating Trident state manager.");
			stateManager.activate();
			logger.info("Rebalancing the shard state manager.");
			stateManager.rebalance(taskIndex, totalNumTasks);
			isNotPrepared = false;
		} catch (Exception ex) {
			logger.warn("Error preparing Kinesis state manager.", ex);
		}
	}

	/**
	 * Creates a copy of the record so we don't get interference from bolts that
	 * execute in the same JVM. We invoke ByteBuffer.duplicate() so the
	 * ByteBuffer state is decoupled.
	 * 
	 * @param record
	 *            Kinesis record
	 * @return Copied record.
	 */
	private Record copyRecord(Record record) {
		Record duplicate = new Record();
		duplicate.setPartitionKey(record.getPartitionKey());
		duplicate.setSequenceNumber(record.getSequenceNumber());
		duplicate.setData(record.getData().duplicate());
		return duplicate;
	}

}
