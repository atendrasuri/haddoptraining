package com.sapient;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

public class NYSEAssignment {
	
	public static void main(String args[])throws Exception
	{
		SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL basic example")
				.getOrCreate();
		
		JavaRDD<Stock> nyseRDD = spark.read().textFile("C:\\BigData\\NYSE.csv").javaRDD().map(line -> {
			String[] parts = line.split(",");
			Stock stock= new Stock();
			stock.setName(parts[1]);
			stock.setStock(Double.parseDouble(parts[6]));
			return stock;
		});
		
		JavaRDD<Stock> sortRDD =nyseRDD.sortBy(new Function<Stock, Double>() {

			@Override
			public Double call(Stock st) throws Exception {
				// TODO Auto-generated method stub
				return st.getStock();
			}
			
		},false,10);
		
		
		JavaPairRDD<String,Double>maxPriceSymbolRDD=sortRDD.mapToPair(new PairFunction<Stock, String, Double>() {

			@Override
			public Tuple2<String, Double> call(Stock s) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Double>(s.getName(),s.getStock());
			}
		}).reduceByKey(new Function2<Double, Double, Double>() {
			
			@Override
			public Double call(Double d1, Double d2) throws Exception {
				// TODO Auto-generated method stub
				return d2>d1?d2:d1;
			}
		});
		
		System.out.println(maxPriceSymbolRDD.collect());
	}

}
