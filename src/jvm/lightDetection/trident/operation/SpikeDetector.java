package lightDetection.trident.operation;

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
public class SpikeDetector implements Aggregator<Map<String, String>> {
	private float spikeThreshold = 0.03f;
	
	public SpikeDetector() {
		
	}
	
	public SpikeDetector(float spikeThreshold) {
		this.spikeThreshold = spikeThreshold;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, String> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, String>();
	}

	@Override
	public void aggregate(Map<String, String> val, TridentTuple tuple, TridentCollector collector) {
		String device_id = tuple.getString(0);
		Object thing = tuple.get(1);
		HashMap<String, List<Double>> map;
		if(thing instanceof HashMap<?,?>)
			map = (HashMap<String, List<Double>>) thing;
		else return;
		List<Double> pair = map.get(device_id);
		double avg = pair.get(0);
		double last_val = pair.get(1);
		String msg = ": avg = " + avg + "   last_val = " + last_val;
		if (Math.abs(last_val - avg) > spikeThreshold * avg)
			msg += "   spike detected at : " + (new DateTime()).toString();
		val.put(device_id, msg);
	}

	@Override
	public void complete(Map<String, String> val, TridentCollector collector) {
		System.out.println("SpikeDetector: " + val);
		collector.emit(new Values(val));
	}
}
