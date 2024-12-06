/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.index.codec.fuzzy;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class PointsVsDocValuesBenchmark {

    private IndexWriter writer;
    private IndexReader reader;
    private IndexSearcher searcher;
    private Directory directory;

    @Param({"1000000"})
    private int numDocs;
    private SimpleLeafCollector collector;
    private Bits liveDocs;

    @Setup
    public void setup() throws IOException {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File indexDir = new File(tmpDir, "lucene-points-benchmark-1");
        if (!indexDir.exists()) {
            indexDir.mkdirs();
        }

        directory = FSDirectory.open(indexDir.toPath());
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setCodec(new Lucene912Codec());
        writer = new IndexWriter(directory, config);

        // Index documents
        Random random = new Random();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            int hour = getRandomHour();
            int port = 10000 + random.nextInt(10000);
            int status = random.nextInt(2) == 0 ? 200 : 500;

            // Index as point
            doc.add(new IntPoint("timestamp-status", hour, port, status));
            doc.add(new IntPoint("status", status));
            doc.add(new IntPoint("timestamp", hour));
            doc.add(new IntPoint("port", port));

            // Index as DocValues
            doc.add(new SortedNumericDocValuesField("status_dv", status));
            doc.add(new SortedNumericDocValuesField("timestamp_dv", status));
            doc.add(new SortedNumericDocValuesField("port_dv", port));

            writer.addDocument(doc);
        }

        writer.commit();
        writer.forceMerge(1);
        reader = DirectoryReader.open(writer);
        searcher = new IndexSearcher(reader);
        // Setup collector that just counts matches
        collector = new SimpleLeafCollector();
        liveDocs = new Bits.MatchAllBits(reader.maxDoc());
    }

    private static int getRandomHour() {
        int minHour = 1000000;
        int maxHour = 2000000;
        Random random = new Random();
        return random.nextInt(maxHour - minHour + 1) + minHour;
    }

    public static class SimpleLeafCollector implements LeafCollector {
        private int count = 0;

        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) {
            count++;
        }

        public int getCount() {
            return count;
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        reader.close();
        writer.close();
        directory.close();
    }

    @Benchmark
    public void queryStatus200MultiPoints(Blackhole blackhole) throws IOException {
        int[] min = new int[]{1000000, 0, 200};
        int[] max = new int[]{2000000, 20000, 200};
        Query query = IntPoint.newRangeQuery("timestamp-status", min, max);
        query(blackhole, query);
    }

    @Benchmark
    public void queryTimestampMultiPoints(Blackhole blackhole) throws IOException {
        int[] min = new int[]{1000000, 0, 200};
        int[] max = new int[]{1200000, 20000, 200};
        Query query = IntPoint.newRangeQuery("timestamp-status", min, max);
        query(blackhole, query);
    }

    @Benchmark
    public void queryStatus200PointsSingleField(Blackhole blackhole) throws IOException {
        int[] min = new int[]{200};
        int[] max = new int[]{200};
        Query query = IntPoint.newRangeQuery("status", min, max);
        query(blackhole, query);
    }

    private void query(Blackhole blackhole, Query query) throws IOException {
        Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1f);
        BulkScorer bulkScorer = weight.bulkScorer(reader.leaves().get(0));
        collector = new SimpleLeafCollector();
        bulkScorer.score(collector, liveDocs, 0, reader.maxDoc());
        blackhole.consume(collector.getCount());
    }


    @Benchmark
    public void queryStatus200DocValues(Blackhole blackhole) throws IOException {
        Query query = SortedNumericDocValuesField.newSlowRangeQuery("status_dv", 200, 200);
        query(blackhole, query);
    }

    @Benchmark
    public void queryTimestampDocValues(Blackhole blackhole) throws IOException {
        Query query = SortedNumericDocValuesField.newSlowRangeQuery("timestamp_dv", 1000000, 1200000);
        query(blackhole, query);
    }

    @Benchmark
    public void queryTimestampPointsSingleField(Blackhole blackhole) throws IOException {
        int[] min = new int[]{1000000};
        int[] max = new int[]{1200000};
        Query query = IntPoint.newRangeQuery("timestamp", min, max);
        query(blackhole, query);
    }

    @Benchmark
    public void queryStatus500AndPort15000Points(Blackhole blackhole) throws IOException {
        int[] min = new int[]{1000000, 14000, 500};
        int[] max = new int[]{2000000, 15000, 500};
        Query query = IntPoint.newRangeQuery("timestamp-status", min, max);
        query(blackhole, query);
    }

    @Benchmark
    public void queryStatus500AndPort15000DocValues(Blackhole blackhole) throws IOException {
        Query statusQuery = SortedNumericDocValuesField.newSlowRangeQuery("status_dv", 500, 500);
        Query portQuery = SortedNumericDocValuesField.newSlowRangeQuery("port_dv", 14000, 15000);

        BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
        bqBuilder.add(statusQuery, BooleanClause.Occur.MUST);
        bqBuilder.add(portQuery, BooleanClause.Occur.MUST);
        Query query = bqBuilder.build();

        query(blackhole, query);
    }

    @Benchmark
    public void queryStatus500AndPort15000PointsSingleField(Blackhole blackhole) throws IOException {
        Query statusQuery = IntPoint.newRangeQuery("status", 500, 500);
        Query portQuery = IntPoint.newRangeQuery("port", 14000, 15000);

        BooleanQuery.Builder bqBuilder = new BooleanQuery.Builder();
        bqBuilder.add(statusQuery, BooleanClause.Occur.MUST);
        bqBuilder.add(portQuery, BooleanClause.Occur.MUST);
        Query query = bqBuilder.build();

        query(blackhole, query);
    }
    @Benchmark
    public void queryStatus500PointsSingleField(Blackhole blackhole) throws IOException {
        Query statusQuery = IntPoint.newRangeQuery("status", 500, 500);
        query(blackhole, statusQuery);
    }

    @Benchmark
    public void queryStatus15000portsSingleField(Blackhole blackhole) throws IOException {
        Query portQuery = IntPoint.newRangeQuery("port", 14000, 15000);
        query(blackhole, portQuery);
    }
}
