package mailAnalyserPackage;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * keeps track of statistics needed to compute frequency and kappa
 * @author Luke Barnett 1109967
 * @author Tony Chen 1111377
 *
 */
public class StatisticsBolt extends BaseRichBolt {

	private static final long serialVersionUID = -3382457336041546604L;
	private OutputCollector collector;
	private final int REPORTING_FREQUENCY;
	private final int NUMBER_OF_CLASSES;
	private long totalCount;
	private long totalPredictedCorrectly;
	
	//array to keep statistics needed for kappa and accuracy
	//[0] are the actual class counts
	//[1] are the predicted class counts
	private double[][] stats;
	private String falseClass;
	
	// Confusion Matrix
	private int[][] conf;
	
	
	public StatisticsBolt(int numberOfClasses, int reportingFrequency){
		REPORTING_FREQUENCY = reportingFrequency;
		NUMBER_OF_CLASSES = numberOfClasses;
		stats = new double[2][numberOfClasses];	
		conf = new int[numberOfClasses][numberOfClasses];
		falseClass = "";
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String label = input.getString(2);
		double score;
		
		if(input.getValue(0).getClass() == double[].class){
			double[] dist = (double[]) input.getValue(0);
			//wont cast Double objects to int, doing it the long way
			Double d_actual =(Double)input.getValue(1);
			int actual = d_actual.intValue();
			int pred = 0;
			double max = -1;
			for(int i=0;i<dist.length;i++){
				if(dist[i]>max){
					pred = i;
					max = dist[i];

					//score
					score = dist[actual];
				}
			}			
			//update counted statistics
			if(pred == actual){
				totalPredictedCorrectly++;
			}
//			else{
//				System.out.println(input.getString(3));
//				falseClass += input.getString(3) + "\n";
//			}
			stats[0][actual]++;
			stats[1][pred]++;	
			
			//Update Confusion Matrix
			conf[pred][actual]++;
			
			totalCount++;
			collector.ack(input);
			
//			if(totalCount % REPORTING_FREQUENCY == 0){
				double accuracy = (double)totalPredictedCorrectly / (double)totalCount;
				//calculate probably of getting correct prediction by chance
				double randomGuessAccuracy = 0;
				for(int i=0;i<stats[0].length;i++){
					randomGuessAccuracy += (stats[0][i]/totalCount)*(stats[1][i]/totalCount);
				}
				double kappa = (accuracy - randomGuessAccuracy) / (1 - randomGuessAccuracy);
				collector.emit(new Values(totalCount,accuracy, Double.isNaN(kappa) ? 0 : kappa, precision(), recall(), label));
//			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("NumberSeen", "Accuracy","Kappa","precision", "recall","label"));
	}
	
	@Override
    public void cleanup() {
		// TODO: Confusion matrix
		System.out.println("StatisticsBolt:Cleanup");
		
		for(int i=0;i<NUMBER_OF_CLASSES;i++){
			for(int j=0;j<NUMBER_OF_CLASSES;j++){
				System.out.print(conf[i][j]+ "\t");
			}
			System.out.println();
		}
		System.out.println("\nPrecision: "+ precision());
		System.out.println("Recall: "+ recall());
		System.out.println("F score: "+ fscore());
		
		System.out.println("Accuracy: " + (double)totalPredictedCorrectly / (double)totalCount);
//		System.out.println(falseClass);
    }    
	

	
	double precision() {
	double tmpPrec = 0.0;
	
	for(int i=0;i<NUMBER_OF_CLASSES-1;i++) { //for each class
		double totalPos = 0.0;
		double truePos = conf[i][i]*1.0;
		for(int j=0;j<NUMBER_OF_CLASSES;j++){
			totalPos += conf[i][j];
		}
		tmpPrec += truePos/totalPos;
	}
	
	return tmpPrec/(NUMBER_OF_CLASSES-1);
	
	}
	
	double recall() {
		double tmpRec = 0.0;
		
		for(int i=0;i<NUMBER_OF_CLASSES-1;i++) { //for each class
			double totalPos = 0.0;
			double truePos = conf[i][i]*1.0;
			for(int j=0;j<NUMBER_OF_CLASSES;j++){
				totalPos += conf[j][i];
			}
			tmpRec += truePos/totalPos;
		}
		
		return tmpRec/(NUMBER_OF_CLASSES-1);
	}

	double fscore() {
		return 2*precision()*recall()/(precision()+recall());
	}
}
