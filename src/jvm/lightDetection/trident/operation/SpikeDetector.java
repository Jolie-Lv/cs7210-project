package lightDetection.trident.operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import backtype.storm.tuple.Values;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * An aggregator that detects spikes based on a constant threshold, the moving average,
 * and the last value seen.
 * @author abhishekchatterjee
 *
 */
public class SpikeDetector implements Aggregator<Map<String, List<String>>> {
	private float spikeThreshold = 0.03f;
	private int partitionId;
    private int numPartitions;
	
	public SpikeDetector() {
		
	}
	
	public SpikeDetector(float spikeThreshold) {
		this.spikeThreshold = spikeThreshold;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		this.partitionId = context.getPartitionIndex();
        this.numPartitions = context.numPartitions();
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, List<String>> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, List<String>>();
	}

	@Override
	public void aggregate(Map<String, List<String>> val, TridentTuple tuple, TridentCollector collector) {
		String device_id = tuple.getString(0);
		Object thing = tuple.get(1);
		HashMap<String, List<Double>> map;
		if(thing instanceof HashMap<?,?>)
			map = (HashMap<String, List<Double>>) thing;
		else return;
		List<Double> triple = map.get(device_id);
		double avg = triple.get(0);
		double last_val = triple.get(1);
		double last_time = triple.get(2);
		List<String> msg = new ArrayList<String>();
		msg.add(String.format("Spike Detector [%s/%s]", partitionId, numPartitions));
		msg.add(String.format("%s", avg));
		msg.add(String.format("%s", last_val));
		msg.add(String.format("%s", last_time));
		if (Math.abs(last_val - avg) > spikeThreshold * avg) {
			msg.add("spike detected at : " + (new DateTime()).toString());
			val.put(device_id, msg);
		}
	}

	@Override
	public void complete(Map<String, List<String>> val, TridentCollector collector) {
		//System.out.println(val);
		collector.emit(new Values(val));
	}
}
