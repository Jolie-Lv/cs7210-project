package movingAverageWithSpikeDetection;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class InputStreamSpout implements IRichSpout {
	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;
	private int count = 1000000;  
	private String deviceID = "Arduino";
	
	private final Random random = new Random();

	public boolean isDistributed() {
		return true;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
			final SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {		
		if (count-- > 0) {
			/**
			 * Trying to reduce the number of spikes in this fake stream.
			 */
			double randomometer = random.nextDouble();
			double tolerance = 0.999; //Change this to limit the number of spikes that appear.
			if(randomometer - tolerance >= 0)
				randomometer *= 10.0;
			else
				randomometer *= 2.0;
			collector.emit(new Values(deviceID, randomometer + 50));
			//collector.emit(new Values(deviceID, (random.nextDouble() * 10) + 50));			
		} else if (count-- == -1) {
			collector.emit(new Values(deviceID, -1.0));
		}
		/**
		 * Comment out this block if you want instant results instead of plausible ones.
		 */
		try {
			Thread.sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
	}

	@Override
	public void ack(final Object id) {
	}

	@Override
	public void fail(final Object id) {
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("string","double"));
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}