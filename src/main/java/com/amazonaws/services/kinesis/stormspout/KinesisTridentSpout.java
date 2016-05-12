package com.amazonaws.services.kinesis.stormspout;

import java.util.Map;

import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * Trident spout to receive streams from Amazon Kinesis
 * 
 * @author yeminkhaung
 *
 */
public class KinesisTridentSpout implements ITridentSpout<Map<String, Object>> {

	/** Generated serial ID */
	private static final long serialVersionUID = 4219717160998069057L;

	private KinesisSpoutConfig config;

	private InitialPositionInStream initialPosition;

	private KinesisShardGetterBuilder getterBuilder;

	private IShardListGetter shardListGetter;

	/**
	 * @param config
	 * @param credentialsProvider
	 * @param clientConfiguration
	 */
	public KinesisTridentSpout(KinesisSpoutConfig config,
			AWSCredentialsProvider credentialsProvider,
			ClientConfiguration clientConfiguration) {
		this.config = config;
		KinesisHelper helper = new KinesisHelper(config.getStreamName(),
				credentialsProvider, clientConfiguration, config.getRegion());
		shardListGetter = helper;
		getterBuilder = new KinesisShardGetterBuilder(config.getStreamName(),
				helper, config.getMaxRecordsPerCall(),
				config.getEmptyRecordListBackoffMillis());
		initialPosition = config.getInitialPositionInStream();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public BatchCoordinator getCoordinator(String txstateid, Map conf,
			TopologyContext context) {
		KinesisTridentCoordinator coordinator = new KinesisTridentCoordinator();
		return coordinator;
	}

	@Override
	public Emitter getEmitter(String txStateId, Map conf,
			TopologyContext context) {
		KinesisTridentEmitter emitter = new KinesisTridentEmitter(config,
				shardListGetter, getterBuilder, initialPosition);
		emitter.init(txStateId, conf, context);
		return emitter;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("messageKey", "messageValue");
	}
}
