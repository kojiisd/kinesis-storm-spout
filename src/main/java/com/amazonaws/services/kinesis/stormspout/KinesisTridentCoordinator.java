package com.amazonaws.services.kinesis.stormspout;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;

/*
 * Coordinates transaction operations for Kinesis Trident Spout 
 *
 * @author yeminkhaung
 *
 */
public class KinesisTridentCoordinator implements
		BatchCoordinator<Map<String, Object>> {

	/** Logger */
	private static final Logger logger = LoggerFactory
			.getLogger(KinesisTridentCoordinator.class);

	@Override
	public void close() {
		// Do Nothing.
	}

	@Override
	public Map<String, Object> initializeTransaction(long txid,
			Map<String, Object> prevMetadata, Map<String, Object> currMetadata) {
		logger.debug(
				"initializeTransaction : txid={}, prevMetadata={}, currMetadata={}",
				new Object[] { txid, prevMetadata, currMetadata });
		return null;
	}

	@Override
	public boolean isReady(long txid) {
		return true;
	}

	@Override
	public void success(long txid) {
		logger.debug("Transaction is succeeded : txis={0}", txid);
	}

}
