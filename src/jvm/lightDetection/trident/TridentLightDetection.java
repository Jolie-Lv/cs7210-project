package lightDetection.trident;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

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
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;

/**
 * 
 * @author abhishekchatterjee
 *
 */
public class TridentLightDetection {
	public static StormTopology buildTopology(LocalDRPC drpc) {
		InputStreamSpout spout = new InputStreamSpout(); //Comment this line and uncomment the next, if using actual Arduino.
		//LightEventSpout spout = new LightEventSpout();
		
		Map<String, LinkedList<Double>> deviceIDtoStreamMap = new HashMap<String, LinkedList<Double>>();
		Map<String, Double> deviceIDtoSumOfEvents = new HashMap<String, Double>();
		
		TridentTopology topology = new TridentTopology();
		TridentState countState = topology.newStream("spout", spout)
				.groupBy(new Fields("device_id"))
				.aggregate(new Fields("device_id", "value"),
						new DeviceAggregator(),
						new Fields("device_vals"))
				.parallelismHint(2)
				.groupBy(new Fields("device_id"))
				.aggregate(new Fields("device_id", "device_vals"),
						new MovingAverage(),
						new Fields("device_average"))
				.parallelismHint(2)
				.shuffle()
				.groupBy(new Fields("device_id"))
				.aggregate(new Fields("device_id", "device_average"),
						new SpikeDetector(),
						new Fields("device_spikes"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
				.parallelismHint(2);
		
		topology.newDRPCStream("lumos", drpc)
			.stateQuery(countState, new Fields("args"), new MapGet(), new Fields("count"));
		
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
            for (int i = 0; i < 100; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("lumos", null));
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
