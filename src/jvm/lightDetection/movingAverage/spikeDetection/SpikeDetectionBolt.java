package lightDetection.movingAverage.spikeDetection;


import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.joda.time.DateTime;

public class SpikeDetectionBolt implements IBasicBolt {

	private static final long serialVersionUID = 1L;
	
	private float spikeThreshold = 0.03f;
	
	public SpikeDetectionBolt() {
	}
	
	public SpikeDetectionBolt(float spikeThreshold) {
		this.spikeThreshold = spikeThreshold;
	}
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context) {
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		final String deviceID = tuple.getString(0);
		final double movingAverageInstant = tuple.getDouble(2);
		final double nextDouble = tuple.getDouble(1);
		if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
			collector.emit(new Values(deviceID, movingAverageInstant, nextDouble, "spike detected"));
			
			/**
			 * TODO: ML model update.
			 */
			System.out.println(deviceID + "  " + movingAverageInstant + "   " + nextDouble  + " spike detected at : " + (new DateTime()).toString());
		}
	}

	@Override
	public void cleanup() {
	}
	
	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		//Field types: String, double, double, String
		declarer.declare(new Fields("device_id", "value", "moving_average", "msg"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}