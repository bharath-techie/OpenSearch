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
//import org.apache.lucene.search.DocIdSetIterator;
//import org.apache.lucene.store.IndexInput;
//import org.roaringbitmap.RangeBitmap;
//import org.roaringbitmap.RoaringBitmap;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.CharBuffer;
//import java.nio.DoubleBuffer;
//import java.nio.FloatBuffer;
//import java.nio.IntBuffer;
//import java.nio.LongBuffer;
//import java.nio.ReadOnlyBufferException;
//import java.nio.ShortBuffer;
//
//public class LuceneByteBuffer {
//    public DocIdSetIterator getTimestamp(IndexInput bitsetIn) throws IOException {
//        // Read header
//        int header = bitsetIn.readInt();
//        long min = bitsetIn.readLong();
//
//        // Calculate remaining length (total - header size)
//        long remainingLength = bitsetIn.length() - Integer.BYTES - Long.BYTES;
//
//        // Create ByteBuffer wrapper around IndexInput
//        ByteBuffer buffer = new ByteBuffer() {
//            private long startOffset = 0;
//            private long currentPos = bitsetIn.getFilePointer();
//
//            @Override
//            public byte get() {
//                try {
//                    bitsetIn.seek(startOffset + currentPos);
//                    currentPos++;
//                    return bitsetIn.readByte();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public byte get(int index) {
//                try {
//                    bitsetIn.seek(startOffset + index);
//                    return bitsetIn.readByte();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer slice() {
//                return this;
//            }
//
//            @Override
//            public ByteBuffer slice(int index, int length) {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public ByteBuffer duplicate() {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public ByteBuffer asReadOnlyBuffer() {
//                return this;  // Already effectively read-only
//            }
//
//            @Override
//            public ByteBuffer put(byte b) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public ByteBuffer put(int index, byte b) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public ByteBuffer compact() {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public boolean isReadOnly() {
//                return true;
//            }
//
//            @Override
//            public boolean isDirect() {
//                return false;
//            }
//
//            @Override
//            public char getChar() {
//                try {
//                    bitsetIn.seek(startOffset + currentPos);
//                    currentPos += 2;
//                    return (char) bitsetIn.readByte();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putChar(char value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public char getChar(int index) {
//                try {
//                    bitsetIn.seek(startOffset + index);
//                    return (char) bitsetIn.readByte();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putChar(int index, char value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public CharBuffer asCharBuffer() {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public short getShort() {
//                try {
//                    bitsetIn.seek(startOffset + currentPos);
//                    currentPos += 2;
//                    return bitsetIn.readShort();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putShort(short value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public short getShort(int index) {
//                try {
//                    bitsetIn.seek(startOffset + index);
//                    return bitsetIn.readShort();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putShort(int index, short value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public ShortBuffer asShortBuffer() {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public int getInt() {
//                try {
//                    bitsetIn.seek(startOffset + currentPos);
//                    currentPos += 4;
//                    return bitsetIn.readInt();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putInt(int value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public int getInt(int index) {
//                try {
//                    bitsetIn.seek(startOffset + index);
//                    return bitsetIn.readInt();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putInt(int index, int value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public IntBuffer asIntBuffer() {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public long getLong() {
//                try {
//                    bitsetIn.seek(startOffset + currentPos);
//                    currentPos += 8;
//                    return bitsetIn.readLong();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putLong(long value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public long getLong(int index) {
//                try {
//                    bitsetIn.seek(startOffset + index);
//                    return bitsetIn.readLong();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putLong(int index, long value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public LongBuffer asLongBuffer() {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public float getFloat() {
//                try {
//                    bitsetIn.seek(startOffset + currentPos);
//                    currentPos += 4;
//                    return Float.intBitsToFloat(bitsetIn.readInt());
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putFloat(float value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public float getFloat(int index) {
//                try {
//                    bitsetIn.seek(startOffset + index);
//                    return Float.intBitsToFloat(bitsetIn.readInt());
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putFloat(int index, float value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public FloatBuffer asFloatBuffer() {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            public double getDouble() {
//                try {
//                    bitsetIn.seek(startOffset + currentPos);
//                    currentPos += 8;
//                    return Double.longBitsToDouble(bitsetIn.readLong());
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putDouble(double value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public double getDouble(int index) {
//                try {
//                    bitsetIn.seek(startOffset + index);
//                    return Double.longBitsToDouble(bitsetIn.readLong());
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public ByteBuffer putDouble(int index, double value) {
//                throw new ReadOnlyBufferException();
//            }
//
//            @Override
//            public DoubleBuffer asDoubleBuffer() {
//                throw new UnsupportedOperationException();
//            }
//        };
//
//        RangeBitmap bitmap = RangeBitmap.map(buffer);
//        RoaringBitmap a = bitmap.gt(0);
//        return a.iterator();
//    }
//}
