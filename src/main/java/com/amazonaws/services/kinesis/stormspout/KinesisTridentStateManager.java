package com.amazonaws.services.kinesis.stormspout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.stormspout.state.zookeeper.ZookeeperStateManager;

/**
 * @author yeminkhaung
 *
 */
public class KinesisTridentStateManager extends ZookeeperStateManager {

	/** Logger */
	private static final Logger logger = LoggerFactory
			.getLogger(KinesisTridentStateManager.class);

	/**
	 * BatchId index for splitted Message Id array
	 */
	public static final int BATCH_SPLIT = 0;

	/**
	 * ShardId index for splitted Message Id array
	 */
	public static final int SHARD_SPLIT = 1;

	/**
	 * SequenceId index for splitted Message Id array
	 */
	public static final int SEQ_SPLIT = 2;

	/**
	 * Parameterized constructor
	 * 
	 * @param config
	 * @param shardListGetter
	 * @param getterBuilder
	 * @param initialPosition
	 */
	public KinesisTridentStateManager(KinesisSpoutConfig config,
			IShardListGetter shardListGetter,
			IShardGetterBuilder getterBuilder,
			InitialPositionInStream initialPosition) {
		super(config, shardListGetter, getterBuilder, initialPosition);
	}

	/**
	 * Deactivate Kinesis State manager
	 * 
	 * @throws InterruptedException
	 */
	public void deactivateStateManager() throws InterruptedException {
		super.deactivate();
	}

	/**
	 * Split message Id and get the Id of specified index
	 * 
	 * @param messageId
	 *            message Id
	 * @param splitter
	 *            splitter token
	 * @param splitPart
	 *            index of result Id (BatchId=0, ShardId=1, SequenceId=2)
	 * @return Result Id string
	 */
	public static String splitMessageId(String messageId, String splitter,
			int splitPart) {
		String requiredSeg = null;
		try {
			String[] parts = messageId.split(splitter);
			requiredSeg = parts[splitPart];
		} catch (Exception ex) {
			logger.debug("Message {} cannot be parse.", messageId, ex);
		}
		return requiredSeg;
	}

	/**
	 * Create message Id from Batch, Shard, Sequence ID
	 * 
	 * @param batchId
	 *            Batch Id
	 * @param shardId
	 *            Shard Id
	 * @param seqNum
	 *            Sequence Id
	 * @return Merged Message Id
	 */
	public static String makeMessageId(String batchId, String shardId,
			String seqNum) {
		return new StringBuilder(batchId).append(":").append(shardId)
				.append(":").append(seqNum).toString();
	}
}
