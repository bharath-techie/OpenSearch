/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.startree.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/** Query class for querying star tree data structure */
public class StarTreeQuery extends Query implements Accountable {
    private static final Logger logger = LogManager.getLogger(StarTreeQuery.class);


    Map<String, List<Predicate<Long>>> compositePredicateMap;
    Set<String> groupByColumns;
    Map<String, List<String>> keywordList;

    Map<String, List<Long>> compositeMap;
    public StarTreeQuery(Map<String, List<Predicate<Long>>> compositePredicateMap, Map<String, List<String>> keywordList,
        Set<String> groupByColumns) {
        this.compositePredicateMap = compositePredicateMap;
        this.groupByColumns = groupByColumns;
        this.keywordList = keywordList;
    }

    @Override
    public String toString(String field) {
        return null;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
        return sameClassAs(obj);
    }

    @Override
    public int hashCode() {
        return classHash();
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        //logger.info("Query ::: createWeight ::: size: {}", compositePredicateMap.size());
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                //logger.info("Query ::: scorer ::: size: {}", compositePredicateMap.size());
                Object obj = context.reader().getAggregatedDocValues();
                //context.reader().getFieldInfos().fieldInfo("clientip");
                SortedSetDocValues field = context.reader().getSortedSetDocValues("clientip");
                Map<String, List<Predicate<Long>>> concurrentHashMap = new ConcurrentHashMap<String, List<Predicate<Long>>>();
                concurrentHashMap.putAll(compositePredicateMap != null ? compositePredicateMap : new HashMap<>());
                DocIdSetIterator result = null;
                if (obj != null) {
                    StarTreeAggregatedValues val = (StarTreeAggregatedValues) obj;
                    final Map<String, List<Predicate<Long>>> keywordOrdMap =
                        getKeywordOrdMap(context, keywordList, val.keywordDimValues, field);
                    final Map<String, List<Long>> keywordOrdMap1 =
                        getKeywordOrdLongMap(context, keywordList, val.keywordDimValues, field);
                    if(keywordOrdMap != null) {
                        concurrentHashMap.putAll(keywordOrdMap);
                    }
                    if(keywordOrdMap1 != null) {
                        compositeMap = new ConcurrentHashMap<>();
                        compositeMap.putAll(keywordOrdMap1);
                    } else {
                        compositeMap = new ConcurrentHashMap<>();
                    }
//                    System.out.println(String.format("Query ::: keywordOrdMap size : %d , thread : %s" , keywordOrdMap != null ? keywordOrdMap.size() : 0,
//                        Thread.currentThread().getName()));

                    final StarTreeFilter filter = new StarTreeFilter(val, concurrentHashMap, compositeMap, groupByColumns);
                    result = filter.getStarTreeResult(concurrentHashMap);
                    if (filter._result.maxMatchedDoc != -1) {
                        logger.info("Query ::: keywordOrdMap size : {}" , keywordOrdMap != null ? keywordOrdMap.size() : 0);
                    } else {
                        logger.info("No matches");
                    }
                }
                return new ConstantScoreScorer(this, score(), scoreMode, result);

            }

            Map<String, List<Predicate<Long>>> getKeywordOrdMap(final LeafReaderContext context, final Map<String, List<String>> keywordList,
                final Map<String, SortedSetDocValues> keywordMap, final SortedSetDocValues field)
                throws IOException {
                Map<String, List<Predicate<Long>>> keywordOrdMap = new ConcurrentHashMap<>();
                for(Map.Entry<String, List<String>> keyword : keywordList.entrySet()) {
                    final List<Predicate<Long>> predicates = keywordOrdMap.getOrDefault(keyword.getKey(), new ArrayList<>());
                    for(String val : keyword.getValue()) {
                        BytesRef bytesRef = getBytesRefForField(val, keyword.getKey(), context);
                        long ord = keywordMap.get(keyword.getKey()).lookupTerm(bytesRef);
                        long fieldOrd = field.lookupTerm(bytesRef);
                        final SegmentReader segmentReader = Lucene.segmentReader(context.reader());
                        SegmentCommitInfo info = segmentReader.getSegmentInfo();
//                        logger.info("Query ::: ORD : {} , fieldOrd :{}" , ord, fieldOrd);
                        if(ord > 0) {
                            logger.info("Query ::: ORD : {} , fieldOrd :{} , IP : {} " , ord, fieldOrd, val);
                            Predicate<Long> predicate = dimVal -> dimVal == ord;
                            predicates.add(predicate);
                        }
                    }
                    if(predicates.size() > 0) {
                        keywordOrdMap.put(keyword.getKey(), predicates);
                    } else {
                        return null;
                    }
                }
                return keywordOrdMap;
            }

            Map<String, List<Long>> getKeywordOrdLongMap(final LeafReaderContext context, final Map<String, List<String>> keywordList,
                final Map<String, SortedSetDocValues> keywordMap, final SortedSetDocValues field)
                throws IOException {
                Map<String, List<Long>> keywordOrdMap = new ConcurrentHashMap<>();
                for(Map.Entry<String, List<String>> keyword : keywordList.entrySet()) {
                    final List<Long> predicates = keywordOrdMap.getOrDefault(keyword.getKey(), new ArrayList<>());
                    for(String val : keyword.getValue()) {
                        BytesRef bytesRef = getBytesRefForField(val, keyword.getKey(), context);
                        long ord = keywordMap.get(keyword.getKey()).lookupTerm(bytesRef);
                        long fieldOrd = field.lookupTerm(bytesRef);
                        final SegmentReader segmentReader = Lucene.segmentReader(context.reader());
                        SegmentCommitInfo info = segmentReader.getSegmentInfo();
                      //  logger.info("Query ::: ORD : {} , fieldOrd :{}" , ord, fieldOrd);
                        if(ord > 0) {
//                            System.out.println(String.format("Query ::: ORD : %d , fieldOrd : %d , IP : %s , ContextSeg : %s, thread : %s " , ord, fieldOrd, val,
//                                info.info.name, Thread.currentThread().getName()));
//                            logger.info("Query ::: ORD : {} , fieldOrd :{} , IP : {} , ContextSeg : {}, thread " , ord, fieldOrd, val,
//                                info.info.name, Thread.currentThread().getName());
                            predicates.add(ord);
                        }
                    }
                    if(predicates.size() > 0) {
                        keywordOrdMap.put(keyword.getKey(), predicates);
                    } else {
                        return null;
                    }
                }
                return keywordOrdMap;
            }

            BytesRef getBytesRefForField(final String term, final String field, final LeafReaderContext context) {
                if(field.contains("ip")) {
                    return new BytesRef(InetAddressPoint.encode(InetAddresses.forString(term)));
                }
                return BytesRef.deepCopyOf(BytesRefs.toBytesRef(term));
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }
}
