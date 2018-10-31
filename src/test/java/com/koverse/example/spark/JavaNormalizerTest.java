/*
 * Copyright 2016 Koverse, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.koverse.example.spark;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.koverse.sdk.data.SimpleRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * These tests leverage the great work at https://github.com/holdenk/spark-testing-base
 */
public class JavaNormalizerTest extends SharedJavaSparkContext {

  @Test
  public void NormalizerTest() {
    // Create the SimpleRecords we will put in our input RDD
    SimpleRecord record0 = new SimpleRecord();
    SimpleRecord record1 = new SimpleRecord();
    SimpleRecord record2 = new SimpleRecord();
    SimpleRecord record3 = new SimpleRecord();
    SimpleRecord record4 = new SimpleRecord();
    record0.put("text", "are these these these words words are to be counted");
    record1.put("text", "more words  that are worth counting");
    record4.put("text", "more words that are worth counting");
    record2.put("text", "are these these these words words are not to be counted");
    record3.put("text", "yabba dabba doo");
    record3.put("id", "different") ;
    record4.put("id", "same as 1") ;

    List<SimpleRecord> records = Arrays.asList(
        record0,
        record1,
        record2
        //record3, //completely distinct
        //record4  //same as 1
    );


    // Create the input RDD
    JavaRDD<SimpleRecord> inputRDD = jsc().parallelize(records);

    // Create and run the word counter to get the output RDD
    JavaNormalizer normalizer = new JavaNormalizer("text", "['\".?!,:;\\s]+");
    JavaRDD<SimpleRecord> rdd = normalizer.createIds(inputRDD);
    normalizer.broadcastDictionary(this.jsc(), normalizer.createDictionary(rdd));

    JavaPairRDD<String, Map<String, Double>> documentVectors = normalizer.createVectors(rdd);
   // documentVectors.foreach( dv -> System.out.println(dv));

    //System.out.println("---  " + record4.get("id"));
    JavaPairRDD<String, Double> similarities = normalizer.similarity(record4, documentVectors);
    List<Tuple2<String, Double>> collect1 = similarities.collect();
    // should be amost 1 (rounding errors) with record3
    //noinspection Convert2MethodRef

    //collect1.forEach( c -> System.out.println(c));
    //System.out.println("---  " + record3.get("id"));
    assertTrue(0.9 < collect1.get(0)._2);
    JavaPairRDD<String, Double> similarities2 = normalizer.similarity(record3, documentVectors);
    List<Tuple2<String, Double>> collect2 = similarities2.collect();
    // should be all zeros
    //noinspection Convert2MethodRef
   // collect2.forEach( c -> System.out.println(c));
    assertTrue(0.000001 > collect2.get(0)._2);
  }
}
