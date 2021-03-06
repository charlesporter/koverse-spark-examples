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

import static org.junit.Assert.*;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import com.koverse.com.google.common.collect.Lists;
import com.koverse.sdk.data.SimpleRecord;
import java.util.Optional;

/**
 * These tests leverage the great work at https://github.com/holdenk/spark-testing-base
 */
public class JavaWordCounterTest  extends SharedJavaSparkContext {

  @Test
  public void rddTest() {
    // Create the SimpleRecords we will put in our input RDD
    SimpleRecord record0 = new SimpleRecord();
    SimpleRecord record1 = new SimpleRecord();
    record0.put("text", "these words are to be counted");
    record0.put("id", 0);
    record1.put("text", "more words   that are worth counting");
    record1.put("id", 1);
    
    // Create the input RDD
    JavaRDD<SimpleRecord> inputRecordsRdd = jsc().parallelize(Lists.newArrayList(record0, record1));
    
    // Create and run the word counter to get the output RDD
    JavaWordCounter wordCounter = new JavaWordCounter("text", "['\".?!,:;\\s]+");
    JavaRDD<SimpleRecord> outputRecordsRdd = wordCounter.count(inputRecordsRdd);
    
    assertEquals(outputRecordsRdd.count(), 10);
    
    List<SimpleRecord> outputRecords = outputRecordsRdd.collect();
    Optional<SimpleRecord> countRecordOptional = outputRecords.stream()
     .filter(record -> record.get("word").equals("are"))
     .findFirst();
   
   assertTrue(countRecordOptional.isPresent());
   assertEquals(countRecordOptional.get().get("count"), 2);
    
  }
}
