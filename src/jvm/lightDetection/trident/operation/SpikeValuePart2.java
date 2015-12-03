package lightDetection.trident.operation;

import java.util.List;

import com.github.pmerienne.trident.ml.core.Instance;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SpikeValuePart2 extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		List<Instance<Double>> duck = (List<Instance<Double>>) tuple.get(0);
		Instance<Double> goose = duck.get(0);
		System.out.println("SpikeValuePart2: " + goose + " ... spike detected!");
		collector.emit(new Values(goose));
	}

}
