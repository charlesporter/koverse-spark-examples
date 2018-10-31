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


import com.koverse.com.google.common.collect.Lists;
import com.koverse.sdk.data.SimpleRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Creates normalized document vectors, and uses those vectors to find similar docs.
 */
public class JavaNormalizer implements java.io.Serializable {

  private static final long serialVersionUID = 1L;
  public static final String VECTOR = "vector";
  public static final String ID = "id";
  private final String textFieldName;
  private final String tokenizationString;

  //TODO would have used a long as token ID, but seems not to be allowed
  // as key in a map embedded in a SimpleRecord
  private Broadcast<Map<String, String>> broadcastDictionary = null;

  public JavaNormalizer(String textFieldName, String tokenizationString) {
    this.textFieldName = textFieldName;
    this.tokenizationString = tokenizationString;
  }

  /**
   * Create a dictionary of terms for vector creation.
   *
   * @param inputRecordsRdd set of records from which to create dictionary
   * @return the dictionary
   */
  public Map<String, String> createDictionary(JavaRDD<SimpleRecord> inputRecordsRdd) {

    // get distinct lower-cased terms
    final JavaRDD<String> terms = inputRecordsRdd.flatMap(r -> {
      final String[] splits = r.get(textFieldName).toString().split(tokenizationString);
      final ArrayList<String> tokens = Lists.newArrayList(splits);
      for (int i = 0; i < tokens.size(); ++i) {
        tokens.set(i, tokens.get(i).toLowerCase());
      }
      return tokens;
    });

    // generate a distinct ID for each term
    // final JavaPairRDD<String, Long> termWithID = terms.zipWithIndex();

    //TODO hmmmm... a contained map in a SimpleRecord can only have Strings for keys.
    //return termWithID.mapToPair((term) ->
    // new Tuple2<>(term._1, term._2.toString())
    // ).collectAsMap();

    // since we cant do that, just use the term as its own id.
    return terms.mapToPair(term ->
        new Tuple2<>(term, term)
    ).collectAsMap();
  }


  /**
   * Create document vectors for an RDD of document records.
   * <br/> requires that broadcast dictionary have been set
   *
   * @param records RDD of documents to create vectors for
   */
  public JavaPairRDD<String, Map<String, Double>> createVectors(JavaRDD<SimpleRecord> records) {
    return records.mapToPair(record ->
        createSingleDocumentVector(record)
    );
  }

  private Tuple2<String, Map<String, Double>> createSingleDocumentVector(SimpleRecord record) {

    final Map<String, Long> termCountVector = new HashMap<>();

    final String[] splits = record.get(textFieldName).toString().split(tokenizationString);
    ArrayList<String> tokens = Lists.newArrayList(splits);
    tokens.forEach(t -> {
      final Map<String, String> dictionary = broadcastDictionary.getValue();
      String id = dictionary.get(t.toLowerCase());
      Long count = termCountVector.get(id);
      if (count == null) {
        count = 1L;
      } else {
        count = count + 1;
      }
      termCountVector.put(id, count);
    });

    //normalize vector to unit length
    final double[] magnitudeA = {0L};
    termCountVector.values().forEach(v ->
        magnitudeA[0] += v * v
    );
    double magnitude = Math.sqrt(magnitudeA[0]);
    //System.out.println("magnitude is " + magnitude);
    Map<String, Double> normalVector = new HashMap<>();
    termCountVector.forEach((term, count) ->
        normalVector.put(term, count / magnitude)
    );
    return new Tuple2<>(record.get(ID).toString(), normalVector);
  }

  /**
   * Broadcast dictionary to all partitions.
   */
  public void broadcastDictionary(JavaSparkContext ctx, Map<String, String> dictionary) {
    broadcastDictionary = ctx.broadcast(dictionary);
  }

  /**
   * Calculate similarity for subject doc vs all docs in corpus.
   * @return  RDD of doc ids and similarities between 0 and 1
   */
  public JavaPairRDD<String, Double> similarity(SimpleRecord subject,
                                                JavaPairRDD<String, Map<String, Double>> corpus) {
    final Tuple2<String, Map<String, Double>> subjectVector = createSingleDocumentVector(subject);
    //calc distances to all other docs
    JavaPairRDD<String, Double> unorderedDistances = corpus.mapToPair(otherVector ->
        new Tuple2<>(otherVector._1, distance(subjectVector._2, otherVector._2))
    );
    return sortByValue(unorderedDistances);
  }

  private JavaPairRDD<String, Double> sortByValue(JavaPairRDD<String, Double> unorderedDistances) {
    //cant find a sort by value, so exchange, sort, and flip back
    return unorderedDistances.mapToPair(that ->
        new Tuple2<>(that._2, that._1)
    ).sortByKey(
        false
    ).mapToPair(that ->
        new Tuple2<>(that._2, that._1)
    );
  }

  // lol: -- its just an exercise. --
  // using maps to hold vectors probably uses up all the speed advantage of normalizing
  private Double distance(Map<String, Double> subject, Map<String, Double> object) {
    // would be better with IDF, but out of time
    Set<String> allKeys = new HashSet<>(subject.keySet());
    allKeys.addAll(object.keySet());
    double[] accumulator = {0d};
    allKeys.forEach(key ->
        accumulator[0] += valueOrZero(subject, key) * valueOrZero(object, key)
    );
    // no need to divide since normalized
    return accumulator[0];
  }

  private Double valueOrZero(Map<String, Double> map, String key) {
    Double result = map.get(key);
    return null == result ? 0d : result;
  }


  /**
   * convert doc vectors to SimpleRecord's for return to Koverse.
   * @return RDD of SimpleRecord
   */
  public JavaRDD<SimpleRecord> vectorsToSimpleRecords(
                                     JavaPairRDD<String, Map<String, Double>> documentVectors) {
    return documentVectors.map(dv -> {
      SimpleRecord sr = new SimpleRecord();
      sr.put(ID, dv._1);
      sr.put(VECTOR, dv._2);
      return sr;
    });
  }

  /**
   * Creates recordIds if they do not come with them.
   *     This is definitely not a bullet-proof solution
   * @return SimpleRecords with ID field  added
   */
  public JavaRDD<SimpleRecord> createIds(JavaRDD<SimpleRecord> inputRdd) {

    final JavaPairRDD<SimpleRecord, Long> zipWithIndex = inputRdd.zipWithIndex();
    // assign IDs to the simple records
    return zipWithIndex.map(pair -> {
      pair._1.put(ID, pair._2.toString());
      return pair._1;
    } );
  }
}
