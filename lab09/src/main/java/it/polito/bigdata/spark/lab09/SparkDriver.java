package it.polito.bigdata.spark.lab09;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConversions;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;

public class SparkDriver {
	private static double HELPFULNESS_THRESHOLD = 0.9;
	private static double TRAINING_SPLIT = 0.7;
	private static int K = 5;

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		String inputPath = args[0];
		String outputPath = args[1];
		String algorithm = args[2];
		String[] features = new String[args.length-3];
		for (int i=3; i<args.length; i++)
			features[i-3] = args[i];
	
		SparkSession.Builder builder = SparkSession.builder().appName("Lab 9 - A classification pipeline with Spark MLlib");
		try (SparkSession session = builder.getOrCreate(); JavaSparkContext context = new JavaSparkContext(session.sparkContext())) {
		
			/*
			 * Load dataset
			 */
			
			// load as RDD
			JavaRDD<Review> reviews = context.textFile(inputPath)
					// filter out header
					.filter(line -> !line.startsWith("Id,"))
					// select useful columns and generate label
					.map(line -> {
						String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
						int score = Integer.parseInt(fields[6]);
						String text = fields[9];
						double helpfulnessNumerator = Double.parseDouble(fields[4]);
						double helpfulnessDenominator = Double.parseDouble(fields[5]);
						
						double label = -1;
						if (helpfulnessDenominator > 0) {
							double helpfulness = helpfulnessNumerator / helpfulnessDenominator;
							label = helpfulness > HELPFULNESS_THRESHOLD ? 1 : 0;
						}
						
						return new Review(score, text, label);
					});
			
			// convert to DataFrame
			Dataset<Row> dataset = session.createDataFrame(reviews, Review.class);
			
			// split dataset in labeled and unlabeled datasets
			Dataset<Row> labeledDataset = dataset.filter("label >= 0");
			Dataset<Row> unlabeledDataset = dataset.filter("label < 0");
			
	        // split labeled dataset in training and test sets
	        Dataset<Row>[] splits = labeledDataset.randomSplit(new double[] {TRAINING_SPLIT, 1 - TRAINING_SPLIT});
	        Dataset<Row> trainingSet = splits[0].cache();
	        Dataset<Row> testSet = splits[1];
	        
	        /*
	         * Preprocessing
	         */
	        
	        // convert text to lowercase and split in words
	        Tokenizer tokenizer = new Tokenizer()
	        		.setInputCol("text")
	        		.setOutputCol("words");
	
	        // remove stopwords
	        StopWordsRemover stopWordsRemover = new StopWordsRemover()
	        		.setInputCol("words")
	        		.setOutputCol("filteredWords");
	
	        // calculate TF features
	        HashingTF hashingTF = new HashingTF()
	        		.setNumFeatures(1000)
	        		.setInputCol("filteredWords")
	        		.setOutputCol("TF");
	
	        // transform TF features to TF-IDF
	        IDF idf = new IDF()
	        		.setInputCol("TF")
	        		.setOutputCol("TF-IDF");
	
	        // add features by assembling interesting columns
	        VectorAssembler featuresAssembler = new VectorAssembler()
	        		.setInputCols(features)
	        		.setOutputCol("features");
	        
	        // create preprocessing pipeline
	        Pipeline preprocessing = new Pipeline()
	        		.setStages(new PipelineStage[] {tokenizer, stopWordsRemover, hashingTF, idf, featuresAssembler});
	
	        /*
	         * Model
	         */
	        
	        ParamGridBuilder gridBuilder = new ParamGridBuilder();
	        
	        Estimator<?> estimator;
	        switch (algorithm) {
	        case "dt":
	        	DecisionTreeClassifier dt = new DecisionTreeClassifier();
	        	estimator = dt;
	        	gridBuilder.addGrid(dt.impurity(), JavaConversions.iterableAsScalaIterable(Arrays.asList("gini", "entropy")));
	        	break;
	        case "lr":
	        	LogisticRegression lr = new LogisticRegression();
	        	estimator = lr;
	        	gridBuilder.addGrid(lr.maxIter(), new int[] {10, 100, 1000});
	        	gridBuilder.addGrid(lr.regParam(), new double[] {0, 0.01, 0.1});
	        	break;
	        default:
	        	throw new RuntimeException("Unsupported algorithm " + algorithm);
	        }
	
	        /*
	         * Post-processing
	         */
	        
	        // convert predicted label to categorical
	        IndexToString labelConverter = new IndexToString()
	        		.setInputCol("prediction")
	        		.setOutputCol("predictedLabel")
	        		.setLabels(new String[] {"Useless", "Useful"});
	        
	        // create postprocessing pipeline
	        Pipeline postprocessing = new Pipeline()
	        		.setStages(new PipelineStage[] {labelConverter});
	        
	        /*
	         * Train model
	         */
	        
	        Pipeline pipeline = new Pipeline()
	        		.setStages(new PipelineStage[] {preprocessing, estimator, postprocessing});
	        CrossValidator crossValidator = new CrossValidator()
	        		.setEstimator(pipeline)
	        		.setEstimatorParamMaps(gridBuilder.build())
	        		.setEvaluator(new BinaryClassificationEvaluator())
	        		.setNumFolds(K);
	        CrossValidatorModel model = crossValidator.fit(trainingSet);
	        
	        System.out.println("===== CROSS-VALIDATION =====");
	        System.out.println("Algorithm: " + algorithm);
	        PipelineModel pipelineModel = (PipelineModel) model.bestModel();
	        for (Transformer t : pipelineModel.stages()) {
	        	if (t instanceof Model && !(t instanceof PipelineModel))
	        		System.out.println(t.extractParamMap());
	        }
	        System.out.println("============================");
	        
	        /*
	         * Test model
	         */
	        
			Dataset<Row> predictedTestSet = model.transform(testSet).select("prediction", "label");
	        MulticlassMetrics metrics = new MulticlassMetrics(predictedTestSet);
	        System.out.println("===== EVALAUTION =====");
			System.out.println("Accuracy: " + metrics.accuracy());
			System.out.println("F1-score: " + metrics.fMeasure(1));
			System.out.println("Precision: " + metrics.precision(1));
			System.out.println("Recall: " + metrics.recall(1));
			System.out.println("Confusion matrix: \n" + metrics.confusionMatrix());
			System.out.println("======================");
	        
	        /*
	         * Use model
	         */
	        
	        Dataset<Row> predictedUnlabeledDataset = model.transform(unlabeledDataset)
	        		.select("prediction", "predictedLabel", "textLength", "text");
	        predictedUnlabeledDataset.write().option("header", true).csv(outputPath);
		}
	}
}
