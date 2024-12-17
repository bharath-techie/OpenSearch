///*
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// */
//
//package org.opensearch.index.codec.composite.composite912;
//
//import org.apache.lucene.index.IndexFileNames;
//import org.apache.lucene.index.SegmentReadState;
//import org.roaringbitmap.RangeBitmap;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.nio.file.StandardOpenOption;
//
//import static org.opensearch.action.admin.indices.stats.CommonStatsFlags.Flag.Store;
//
//public class RangeBitmapReader implements AutoCloseable {
//    private final FileChannel channel;
//    private final ByteBuffer mappedBuffer;
//    private final RangeBitmap bitmap;
//
//    public RangeBitmapReader(String filePath, long fp, SegmentReadState readState) throws IOException {
//        // Open file in READ-ONLY mode
//        Path path = Paths.get(filePath);
//        IndexFileNames.segmentFileName(readState.segmentInfo.name, readState.segmentSuffix, "rbs");
//        this.channel = FileChannel.open(path, StandardOpenOption.READ);
//        (readState.segmentInfo.dir)
//        // Memory map the entire file
//        this.mappedBuffer = channel.map(
//            FileChannel.MapMode.READ_ONLY,
//            fp,
//            channel.size()
//        );
//
//        // Create RangeBitmap from the mapped buffer
//        this.bitmap = RangeBitmap.map(mappedBuffer);
//        bitmap.between()
//    }
//
//    public RangeBitmap getBitmap() {
//        return bitmap;
//    }
//
//    @Override
//    public void close() throws IOException {
//        channel.close();
//    }
//}
