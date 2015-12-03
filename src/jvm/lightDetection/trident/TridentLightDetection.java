package lightDetection.trident;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.github.pmerienne.trident.ml.preprocessing.InstanceCreator;
import com.github.pmerienne.trident.ml.regression.PARegressor;
import com.github.pmerienne.trident.ml.regression.RegressionQuery;
import com.github.pmerienne.trident.ml.regression.RegressionUpdater;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import lightDetection.movingAverage.spikeDetection.InputStreamSpout;
import lightDetection.movingAverage.spikeDetection.LightEventSpout;
import lightDetection.trident.operation.DeviceAggregator;
import lightDetection.trident.operation.MovingAverage;
import lightDetection.trident.operation.SpikeDetector;
import lightDetection.trident.operation.SpikeValue;
import lightDetection.trident.operation.SpikeValuePart2;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;


import backtype.storm.tuple.Values;

import com.github.pmerienne.trident.ml.core.Instance;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author abhishekchatterjee
 *
 */
public class TridentLightDetection {
	//private static final String[] DRPC_ARGS = {"9.32", "10.32", "11.34", "13.65", "14.09", "17.80", "17.91", "17.94", "20.01", "23.13", "25.55"};
	
	public static class DRPCArgsToInstance extends BaseFunction {

		private static final long serialVersionUID = 1L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String drpc_arg = tuple.getString(0);
			
			double label=Double.parseDouble(drpc_arg);
			double[] features=new double[1];
			features[0] = new Double(5000*Math.sin(label));
			
			Instance<Double> instance=new Instance<Double>(label, features);
			
			collector.emit(new Values(instance));
		}
	}
	
	public static StormTopology buildTopology(LocalDRPC drpc) {
		InputStreamSpout spout = new InputStreamSpout(); //Comment this line and uncomment the next, if using actual Arduino.
		//LightEventSpout spout = new LightEventSpout();
		
		Map<String, LinkedList<Double>> deviceIDtoStreamMap = new HashMap<String, LinkedList<Double>>();
		Map<String, Double> deviceIDtoSumOfEvents = new HashMap<String, Double>();
		
		TridentTopology topology = new TridentTopology();
		
		Double start_millis = new Double(System.currentTimeMillis());
		
		TridentState luxModel = 
				topology.newStream("spout", spout)
				.groupBy(new Fields("device_id"))
				.aggregate(new Fields("device_id", "value"),
						new DeviceAggregator(),
						new Fields("device_vals"))
				.parallelismHint(2)
				.groupBy(new Fields("device_id"))
				.aggregate(new Fields("device_id", "device_vals"),
						new MovingAverage(start_millis),
						new Fields("device_average"))
				.parallelismHint(2)
				.shuffle()
				.groupBy(new Fields("device_id"))
				.aggregate(new Fields("device_id", "device_average"),
						new SpikeDetector(0.0001f),
						new Fields("device_spike_map"))
				.groupBy(new Fields("device_id"))
				.aggregate(new Fields("device_id", "device_spike_map"),
						new SpikeValue(),
						new Fields("spike_instance_aggr"))
				.each(new Fields("spike_instance_aggr"),
						new SpikeValuePart2(),
						new Fields("spike_instance"))
				.partitionPersist(new MemoryMapState.Factory(),
						new Fields("spike_instance"),
						new RegressionUpdater("passive-aggressive",
								new PARegressor()))
				.parallelismHint(2);
		
		topology.newDRPCStream("predict", drpc)
			.each(new Fields("args"), 
					new DRPCArgsToInstance(), 
					new Fields("instance"))
			.stateQuery(luxModel, 
					new Fields("instance"), 
					new RegressionQuery("regression"), 
					new Fields("prediction"))
			.project(new Fields("args", "prediction"));
		
		return topology.build();
	}
	
	public static void main(String[] args) throws Exception {
	    Config conf = new Config();
	    conf.setMaxSpoutPending(20);
	    
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        } else {
        	LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("spike", conf, buildTopology(drpc));
            Double startTime = new Double(System.currentTimeMillis());
            for(int i=0; i<100; i++) {
            	Double drpc_arg = new Double(System.currentTimeMillis() - startTime + 10);
                System.out.println("DRPC RESULT: " + drpc.execute("predict", drpc_arg.toString()));
                Thread.sleep(1000);
            }
            Utils.sleep(600000);
            cluster.killTopology("spike");    
        } 
	    
	    // Delete the following lines and uncomment the above block when you're serious about life.
	    /*
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("spike", conf, buildTopology(null));
        */
	  }
}
