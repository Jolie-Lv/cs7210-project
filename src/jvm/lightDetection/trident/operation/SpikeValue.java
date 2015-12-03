package lightDetection.trident.operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.pmerienne.trident.ml.core.Instance;

import backtype.storm.tuple.Values;
import storm.trident.operation.Aggregator;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class SpikeValue implements Aggregator<List<Instance<Double>>> {
	/*
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String device_id = tuple.getString(0);
		HashMap<String, List<String>> map;
		Object object = tuple.get(1);
		if(object instanceof HashMap<?, ?>)
			map = (HashMap<String, List<String>>) object;
		else return;
		List<Double> spikeValues = new ArrayList<Double>();
		List<String> msgs = map.get(device_id);
		if(msgs.size() > 3) {
			System.out.println("Spike: " + msgs.get(2));
			collector.emit(new Values(new Double(msgs.get(2))));
		}
	}
	*/

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Instance<Double>> init(Object batchId, TridentCollector collector) {
		return new ArrayList<Instance<Double>>();
	}

	@Override
	public void aggregate(List<Instance<Double>> val, TridentTuple tuple, TridentCollector collector) {
		String device_id = tuple.getString(0);
		HashMap<String, List<String>> map;
		Object object = tuple.get(1);
		if(object instanceof HashMap<?, ?>)
			map = (HashMap<String, List<String>>) object;
		else return;
		List<String> msgs = map.get(device_id);
		if(msgs != null && msgs.size() > 3) {
			Double spike_val = new Double(msgs.get(2));
			Double time_millis = new Double(msgs.get(3));
			double[] spike_array = {spike_val};
			Instance<Double> thing = new Instance<Double>(time_millis, spike_array);
			val.add(thing);
		}
	}

	@Override
	public void complete(List<Instance<Double>> val, TridentCollector collector) {
		if(val.size() < 1)
			return;
		//System.out.println("SpikeValue: " + val.get(0));
		collector.emit(new Values(val));
	}
}
