package lightDetection.trident.operation;

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
 * An Aggregator that aggregates all values associated with a particular deviceID.
 * @author abhishekchatterjee
 *
 */
public class DeviceAggregator implements Aggregator<Map<String, List<Double>>> {
	
    //Defining movingAverageWindow here to avoid having to prune List in next Aggregator.
    private int movingAverageWindow = 1000;
    
    public DeviceAggregator() {
    	
    }
    
    public DeviceAggregator(int movingAverageWindow) {
    	this.movingAverageWindow = movingAverageWindow;
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
	public Map<String, List<Double>> init(Object batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		return new HashMap<String, List<Double>>();
	}

	@Override
	public void aggregate(Map<String, List<Double>> val, TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		String deviceID = tuple.getString(0);
		double nextDouble = tuple.getDouble(1);
        List<Double> values = val.get(deviceID);
        values = values == null ? new ArrayList<Double>() : values;
        if(values.size() >= movingAverageWindow)
        	values.remove(0);
        values.add(nextDouble);
        val.put(deviceID, values);
	}

	@Override
	public void complete(Map<String, List<Double>> val, TridentCollector collector) {
		//System.out.println(String.format("DeviceAggregator: Partition %s out of %s partitions aggregated: %s", partitionId, numPartitions, val));
		collector.emit(new Values(val));
	}

}
