/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.DocIdSetBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.index.codec.startree.codec.StarTreeAggregatedValues;
import org.opensearch.index.codec.startree.query.StarTreeQuery;


@LuceneTestCase.SuppressSysoutChecks(bugUrl="stuff gets printed")
public class StarTreeCodec extends LuceneTestCase {

    private static File plaintextDir;
    private static File mixedDir;
    private static File plaintextDir4;
    static int startreesum = 0;
    static int startreetotalhits = 0;

    @BeforeClass
    public static void setUpDirectories() {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        plaintextDir = assureDirectoryExists(new File(tmpDir, "lucene-plaintext-18"));
        plaintextDir4 = assureDirectoryExists(new File(tmpDir, "lucene-plaintext-9"));
        mixedDir = assureDirectoryExists(new File(tmpDir, "lucene-mixed"));
    }

    private static File assureDirectoryExists(File dir) {
        if (!dir.exists()) {
            dir.mkdirs();
        }
        return dir;
    }
    private static int getRandomDay() {
        int minDay = 1;
        int maxDay = 31;
        Random random = new Random();
        return random.nextInt(maxDay - minDay + 1) + minDay;
    }

    private static int getRandomStatus() {
        int[] statusCodes = {200, 201, 202, 300, 301, 302, 400, 401, 403, 404, 500};
        Random random = new Random();
        return statusCodes[random.nextInt(statusCodes.length)];
    }
    private static int getRandomStatus200() {
        int[] statusCodes = {200, 200, 200, 200, 200, 200, 200, 200, 200, 404, 500};
        Random random = new Random();
        return statusCodes[random.nextInt(statusCodes.length)];
    }

    private static int getRandomHour() {
        int minHour = 1000000;
        int maxHour = 2000000;
        Random random = new Random();
        return random.nextInt(maxHour - minHour + 1) + minHour;
    }

    private static int getRandomHour1() {
        int minHour = 0;
        int maxHour = 23;
        Random random = new Random();
        return random.nextInt(maxHour - minHour + 1) + minHour;
    }

    private static int getRandomInt() {
        int minHour = 0;
        int maxHour = 100000;
        Random random = new Random();
        return random.nextInt(maxHour - minHour + 1) + minHour;
    }

    public void test2dPoints()
        throws Exception {
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new org.opensearch.index.codec.startree.codec.StarTreeCodec());
        Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
        System.out.println("Dir : " + plaintextDir.toPath());
        IndexWriter w = new IndexWriter(luceneDir, config);

        Map<Integer, Integer> statusToCountMap = new HashMap<>();
        Map<Integer, Integer> hourToCountMap = new HashMap<>();


        int totalDocs = 1000000;
        int docsAdded = 0;
        int total = 0;
        while (docsAdded < totalDocs) {

            int hour = getRandomHour();
            int hour1 = getRandomHour();
            int day = getRandomDay();
            int status = getRandomStatus200();

            for (int i = 0; i < 100; i++) {
                statusToCountMap.put(status, statusToCountMap.getOrDefault(status, 0) + 1);
                hourToCountMap.put(hour, hourToCountMap.getOrDefault(hour, 0) + 1);
//        if(status>=399 && hour >= 1815804) {
//          total++;
//        }
                if(status==200 && hour > 1800000) {
                    total++;
                }
                Document doc = new Document();
                doc.add(new IntPoint("timestamp-status", hour, hour/24, status));
                doc.add(new SortedNumericDocValuesField("status", status));
                doc.add(new SortedNumericDocValuesField("hour1", hour));
                doc.add(new SortedNumericDocValuesField("day1", hour/24));
                w.addDocument(doc);
            }

            docsAdded += 100;
        }
        for(Map.Entry<Integer, Integer> entry : statusToCountMap.entrySet()) {
            //System.out.println("Status : " + entry.getKey() + " Count : " + entry.getValue());
        }
        for(Map.Entry<Integer, Integer> entry : hourToCountMap.entrySet()) {
            //System.out.println("Hour : " + entry.getKey() + " Count : " + entry.getValue());
        }

        w.flush();
        System.out.println("Expected : " + total);
        //queryPoints(w);
        //queryPoints(w);
        w.forceMerge(1);
        queryPoints(w);
    }

    private void queryPoints(IndexWriter w)
        throws IOException {
        long startTime = System.currentTimeMillis();

        final IndexReader reader = DirectoryReader.open(w);
        final IndexSearcher searcher = newSearcher(reader, false);
        int[] min = {1800000,0,200};
        int[] max = {2500000,2500000,200};
        startTime = System.nanoTime();

        final Query query =
            IntPoint.newRangeQuery("timestamp-status", min, max);
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        // One query has a count of 0, the disjunction count is the other count
        //assertEquals(1, weight.count(reader.leaves().get(0)));


        System.out.println("Count : " + weight.count(reader.leaves().get(0)));
        System.out.println("============== Finished querying point-tree in ms : " +
            (System.nanoTime() - startTime));
        System.out.println("Sum : " + weight.count(reader.leaves().get(0)) * 200);
//    final Query q = IntPoint.newRangeQuery("timestamp-status", min, max);
//    searcher.search(q, getSumCollector());
//    System.out.println("============== Finished querying point-tree in ms : " +
//        (System.currentTimeMillis() - startTime));
    }

    @Test
    public void testStarTree1()
        throws Exception {

        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setUseCompoundFile(false);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new org.opensearch.index.codec.startree.codec.StarTreeCodec());
        Directory luceneDir = FSDirectory.open(plaintextDir.toPath());
        System.out.println("Dir : " + plaintextDir.toPath());
        IndexWriter w = new IndexWriter(luceneDir, config);

        int totalDocs = 1000000;
        int docsAdded = 0;
        long sum = 0;
        while (docsAdded < totalDocs) {

            int hour = getRandomHour();
            int day = getRandomDay();
            int status = getRandomStatus200();

            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("hour1", hour));
                if(status ==500 && hour >= 1800000) {
                    sum += status;
                }
                doc.add(new SortedNumericDocValuesField("day1", hour/24));
                doc.add(new SortedNumericDocValuesField("status", status));
                w.addDocument(doc);
            }

            docsAdded += 100;
        }

        System.out.println("Exepected sum : " + sum);

        w.flush();
        w.forceMerge(1);
        query(w);

        System.out.println("Star tree sum : "+ startreesum + " == Star tree hits : " + startreetotalhits);
    }

    private void query(IndexWriter w)
        throws IOException {

        final IndexReader reader = DirectoryReader.open(w);
        final IndexSearcher searcher = newSearcher(reader, false);


        Map<String, List<Predicate<Long>>> predicateMap = new HashMap<>();
        List<Predicate<Long>> predicates = new ArrayList<>();
        predicates.add(day -> day >= 1800000/24 && day <= 2000000/24);
        predicateMap.put("day1", predicates);
        predicates = new ArrayList<>();
        predicates.add(status -> status == 500);
        predicateMap.put("status", predicates);
        final Query q = new StarTreeQuery(predicateMap, new HashMap<>(), new HashSet<>());
        final Query q1 = new MatchAllDocsQuery();
        //long startTime = System.currentTimeMillis();
        SimpleCollector collector = getAggregationCollector();

        long startTime = System.currentTimeMillis();
        searcher.search(q, collector);
        System.out.println("============== Finished querying star-tree in ms : " +
            (System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();
        searcher.search(q1, getSumCollector());
        System.out.println("Finished querying normal doc values in ms : " +
            (System.currentTimeMillis() - startTime));

    }

    private SimpleCollector getSumCollector() {
        return new SimpleCollector() {
            private LeafReaderContext context;
            private DocIdSetBuilder docsBuilder;
            public int totalHits;
            private final List<MatchingDocs> matchingDocs = new ArrayList<>();
            SortedNumericDocValues dv = null;

            public long sum = 0;
            final class MatchingDocs {

                /** Context for this segment. */
                public final LeafReaderContext context;

                /** Which documents were seen. */
                public final DocIdSet bits;

                /** Total number of hits */
                public final int totalHits;

                /** Sole constructor. */
                public MatchingDocs(LeafReaderContext context, DocIdSet bits, int totalHits) {
                    this.context = context;
                    this.bits = bits;
                    this.totalHits = totalHits;
                }
            }
            @Override
            public void collect(int doc)
                throws IOException {
                docsBuilder.grow(1).add(doc);

                dv.advanceExact(doc);
                dv.nextValue();
                totalHits++;
            }

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
                dv = context.reader().getSortedNumericDocValues("status");
                assert docsBuilder == null;
                docsBuilder = new DocIdSetBuilder(Integer.MAX_VALUE);
                totalHits = 0;
                this.context = context;
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }

//            @Override
//            public void finish() throws IOException {
//                System.out.println("SUM in normal query : " + sum);
//                System.out.println("Total in point query : " + totalHits);
//                matchingDocs.add(new MatchingDocs(this.context, docsBuilder.build(), totalHits));
//                docsBuilder = null;
//                context = null;
//            }
        };
    }

    private SimpleCollector getAggregationCollector() {
        return new SimpleCollector() {
            private LeafReaderContext context;
            private DocIdSetBuilder docsBuilder;
            public int totalHits;

            public long sum;
            private final List<MatchingDocs> matchingDocs = new ArrayList<>();

            private StarTreeAggregatedValues obj = null;


            final class MatchingDocs {

                /** Context for this segment. */
                public final LeafReaderContext context;

                /** Which documents were seen. */
                public final DocIdSet bits;

                /** Total number of hits */
                public final int totalHits;

                /** Sole constructor. */
                public MatchingDocs(LeafReaderContext context, DocIdSet bits, int totalHits) {
                    this.context = context;
                    this.bits = bits;
                    this.totalHits = totalHits;
                }
            }
            @Override
            public void collect(int doc)
                throws IOException {
                docsBuilder.grow(1).add(doc);
                //context.reader().get
                //Object obj = context.reader().getAggregatedDocValues();
                if(obj != null) {
                    SortedNumericDocValues dv = obj.metricValues.get("status_sum");
//                    NumericDocValues dv1 = obj.dimensionValues.get("day");
//                    NumericDocValues dv2 = obj.dimensionValues.get("status");
//                    NumericDocValues dv3 = obj.dimensionValues.get("hour");
                    dv.advanceExact(doc);
//          System.out.println("ID = " + dv.docID() + " Day Value = " + dv1.longValue() + " Hour
                    //  val = " + dv3.longValue()
//              + " Status = " + dv2.longValue() + " Sum =" + dv.longValue());

                    startreesum += dv.nextValue();
                }
                startreetotalhits++;
            }

            @Override
            protected void doSetNextReader(LeafReaderContext context) throws IOException {
                assert docsBuilder == null;

                docsBuilder = new DocIdSetBuilder(Integer.MAX_VALUE);
                totalHits = 0;
                this.context = context;
                obj = (StarTreeAggregatedValues) context.reader().getAggregatedDocValues();
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }

//            @Override
//            public void finish() throws IOException {
//                System.out.println("Star tree sum : "  + sum);
//                matchingDocs.add(new MatchingDocs(this.context, docsBuilder.build(), totalHits));
//                totalHits = 0;
//                docsBuilder = null;
//                context = null;
//            }
        };

    }
}
