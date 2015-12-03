package lightDetection.trident.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import backtype.storm.tuple.Values;
import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * An Aggregator that computes the average value associated with a deviceID over a moving window.
 * Having a List<Double> was the only way I could think of to transmit both the moving average and
 * the last value in the current window to the SpikeDetector aggregator that is called next.
 * @author abhishekchatterjee
 *
 */
public class MovingAverage implements Aggregator<Map<String, List<Double>>> {
	private double sum = 0.0;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, List<Double>> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, List<Double>>();
	}

	@Override
	public void aggregate(Map<String, List<Double>> val, TridentTuple tuple, TridentCollector collector) {
		String device_id = tuple.getString(0);
		HashMap<String, List<Double>> map = (HashMap<String, List<Double>>) tuple.get(1);
		List<Double> values = (ArrayList<Double>) map.get(device_id);
		values = values == null ? new ArrayList<Double>() : values;
		sum = sumOfEvents(values);
		double avg = sum/values.size();
		double last_val = values.get(values.size()-1);
		ArrayList<Double> pair = new ArrayList<Double>();
		pair.add(avg);
		pair.add(last_val);
		val.put(device_id, pair);
	}
	
	private static Double sumOfEvents(List<Double> events) {
		double sum = 0.0;
		for(double event : events)
			sum += event;
		return sum;
	}

	@Override
	public void complete(Map<String, List<Double>> val, TridentCollector collector) {
		//System.out.println(String.format("MovingAverage: %s", val));
		collector.emit(new Values(val));
	}

}
