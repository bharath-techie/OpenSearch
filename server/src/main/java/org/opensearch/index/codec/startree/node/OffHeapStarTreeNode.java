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
package org.opensearch.index.codec.startree.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.RandomAccessInput;

import java.io.IOException;
import java.util.Iterator;
import org.opensearch.index.codec.startree.query.StarTreeQuery;


/** Off heap implementation of {@link StarTreeNode} */
public class OffHeapStarTreeNode implements StarTreeNode {
    public static final int NUM_INT_SERIALIZABLE_FIELDS = 6;
    public static final int NUM_LONG_SERIALIZABLE_FIELDS = 1;
    public static final long SERIALIZABLE_SIZE_IN_BYTES = (Integer.BYTES * NUM_INT_SERIALIZABLE_FIELDS) + (Long.BYTES
        * NUM_LONG_SERIALIZABLE_FIELDS);
    private static final int DIMENSION_ID_OFFSET = 0;
    private static final int DIMENSION_VALUE_OFFSET = DIMENSION_ID_OFFSET + Integer.BYTES;
    private static final int START_DOC_ID_OFFSET = DIMENSION_VALUE_OFFSET + Long.BYTES;
    private static final int END_DOC_ID_OFFSET = START_DOC_ID_OFFSET + Integer.BYTES;
    private static final int AGGREGATE_DOC_ID_OFFSET = END_DOC_ID_OFFSET + Integer.BYTES;
    private static final int FIRST_CHILD_ID_OFFSET = AGGREGATE_DOC_ID_OFFSET + Integer.BYTES;
    private static final int LAST_CHILD_ID_OFFSET = FIRST_CHILD_ID_OFFSET + Integer.BYTES;

    public static final int INVALID_ID = -1;

    private final int _nodeId;
    private final int _firstChildId;
    private static final Logger logger = LogManager.getLogger(OffHeapStarTreeNode.class);


    RandomAccessInput in;

    public OffHeapStarTreeNode(RandomAccessInput in, int nodeId) throws IOException {
        this.in = in;
        _nodeId = nodeId;
        _firstChildId = getInt(FIRST_CHILD_ID_OFFSET);
    }

    private int getInt(int fieldOffset) throws IOException {
        return in.readInt(_nodeId * SERIALIZABLE_SIZE_IN_BYTES + fieldOffset);
    }

    private long getLong(int fieldOffset) throws IOException {
        return in.readLong(_nodeId * SERIALIZABLE_SIZE_IN_BYTES + fieldOffset);
    }

    @Override
    public int getDimensionId() throws IOException {
        return getInt(DIMENSION_ID_OFFSET);
    }

    @Override
    public long getDimensionValue() throws IOException {
        return getLong(DIMENSION_VALUE_OFFSET);
    }

    @Override
    public int getChildDimensionId() throws IOException {
        if (_firstChildId == INVALID_ID) {
            return INVALID_ID;
        } else {
            return in.readInt(_firstChildId * SERIALIZABLE_SIZE_IN_BYTES);
        }
    }

    @Override
    public int getStartDocId() throws IOException {
        return getInt(START_DOC_ID_OFFSET);
    }

    @Override
    public int getEndDocId() throws IOException {
        return getInt(END_DOC_ID_OFFSET);
    }

    @Override
    public int getAggregatedDocId() throws IOException {
        return getInt(AGGREGATE_DOC_ID_OFFSET);
    }

    @Override
    public int getNumChildren() throws IOException {
        if (_firstChildId == INVALID_ID) {
            return 0;
        } else {
            return getInt(LAST_CHILD_ID_OFFSET) - _firstChildId + 1;
        }
    }

    @Override
    public boolean isLeaf() {
        return _firstChildId == INVALID_ID;
    }

    @Override
    public StarTreeNode getChildForDimensionValue(long dimensionValue) throws IOException {
        if (isLeaf()) {
            return null;
        }

        // Specialize star node for performance
        if (dimensionValue == StarTreeNode.ALL) {
            OffHeapStarTreeNode firstNode = new OffHeapStarTreeNode(in, _firstChildId);
            if (firstNode.getDimensionValue() == StarTreeNode.ALL) {
                return firstNode;
            } else {
                return null;
            }
        }

        // Binary search
        int low = _firstChildId;
        int high = getInt(LAST_CHILD_ID_OFFSET);

        while (low <= high) {
            int mid = (low + high) / 2;
            OffHeapStarTreeNode midNode = new OffHeapStarTreeNode(in, mid);
            long midValue = midNode.getDimensionValue();

            if (midValue == dimensionValue) {
                return midNode;
            } else if (midValue < dimensionValue) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return null;
    }

    @Override
    public Iterator<OffHeapStarTreeNode> getChildrenIterator() throws IOException {
        return new Iterator<OffHeapStarTreeNode>() {
            private int _currentChildId = _firstChildId;
            private final int _lastChildId = getInt(LAST_CHILD_ID_OFFSET);

            @Override
            public boolean hasNext() {
                return _currentChildId <= _lastChildId;
            }

            @Override
            public OffHeapStarTreeNode next() {
                try {
                    return new OffHeapStarTreeNode(in, _currentChildId++);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Iterator<OffHeapStarTreeNode> getChildrenIteratorForRange(long min, long max) throws IOException {
        return new Iterator<OffHeapStarTreeNode>() {
            private int _currentChildId = findFirstChildInRange(min);
            private final int _lastChildId = getInt(LAST_CHILD_ID_OFFSET);

            @Override
            public boolean hasNext() {
                return _currentChildId <= _lastChildId;
            }

            @Override
            public OffHeapStarTreeNode next() {
                try {
                    return new OffHeapStarTreeNode(in, _currentChildId++);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private int findFirstChildInRange(long min) throws IOException {

                int left = _firstChildId;
                int right = getInt(LAST_CHILD_ID_OFFSET);

                while (left <= right) {
                    int mid = left + (right - left) / 2;
                    OffHeapStarTreeNode midNode = new OffHeapStarTreeNode(in, mid);
                    long midValue = midNode.getDimensionValue();
                    if (midValue >= min && midValue <= max) {
                        // Found an element within the range
                        // Check if it's the first one
                        if (mid == _firstChildId) {
                            return mid;
                        } else if(new OffHeapStarTreeNode(in, mid-1).getDimensionValue() < min || new OffHeapStarTreeNode(in, mid-1).getDimensionValue() > max) {
                            return mid;
                        }else {
                            right = mid - 1;
                        }
                    } else if (midValue < min) {
                        left = mid + 1;
                    } else {
                        right = mid - 1;
                    }
                }
                logger.info("No element found within the range");
                // No element found within the range
                return -1;
            }
        };
    }

//    @Override
//    public Iterator<OffHeapStarTreeNode> getChildrenIteratorForRange(long min, long max) throws IOException {
//        return new Iterator<OffHeapStarTreeNode>() {
//            private int _currentChildId = findFirstChildInRange(min);
//            private final int _lastChildIdInRange = findLastChildInRange(max);
//
//            @Override
//            public boolean hasNext() {
//                return _currentChildId <= _lastChildIdInRange;
//            }
//
//            @Override
//            public OffHeapStarTreeNode next() {
//                try {
//                    OffHeapStarTreeNode nextChild = new OffHeapStarTreeNode(in, _currentChildId++);
//                    return nextChild;
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//
//            @Override
//            public void remove() {
//                throw new UnsupportedOperationException();
//            }
//
//            private int findFirstChildInRange(long min) throws IOException {
//                int low = _firstChildId;
//                int high = getInt(LAST_CHILD_ID_OFFSET);
//
//                while (low <= high) {
//                    int mid = (low + high) / 2;
//                    OffHeapStarTreeNode midNode = new OffHeapStarTreeNode(in, mid);
//                    long midValue = midNode.getDimensionValue();
//
//                    if (midValue >= min) {
//                        if (midValue == min || midNode.isLeaf()) {
//                            return mid;
//                        }
//                        high = mid - 1;
//                    } else {
//                        low = mid + 1;
//                    }
//                }
//                return INVALID_ID;
//            }
//
//            private int findLastChildInRange(long max) throws IOException {
//
////                int low = _firstChildId, high = getInt(LAST_CHILD_ID_OFFSET), res = -1;
////                while (low <= high) {
////                    // Normal Binary Search Logic
////                    int mid = (low + high) / 2;
////                    if (arr[mid] > x)
////                        high = mid - 1;
////                    else if (arr[mid] < x)
////                        low = mid + 1;
////
////                        // If arr[mid] is same as x,
////                        // we update res and move to
////                        // the right half.
////                    else {
////                        res = mid;
////                        low = mid + 1;
////                    }
////                }
////                return res;
//
//
//                int low = _firstChildId;
//                int high = getInt(LAST_CHILD_ID_OFFSET);
//
//                while (low <= high) {
//                    int mid = (low + high) / 2;
//                    OffHeapStarTreeNode midNode = new OffHeapStarTreeNode(in, mid);
//                    long midValue = midNode.getDimensionValue();
//
//                    if (midValue <= max) {
//                        if (midValue == max || midNode.isLeaf()) {
//                            return mid;
//                        }
//                        low = mid + 1;
//                    } else {
//                        high = mid - 1;
//                    }
//                }
//                return low - 1;
//            }
//        };
//    }
}
