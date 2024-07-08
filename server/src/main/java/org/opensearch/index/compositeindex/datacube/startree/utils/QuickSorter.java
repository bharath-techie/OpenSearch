/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

/**
 * Copy of it.unimi.dsi.fastutil.Arrays.quickSort
 * We are using this to sort the star-tree doc ids array based on values in off-heap
 * */
public class QuickSorter {

    /** it.unimi.dsi.fastutil.Arrays.quickSort - copy */
    public static void quickSort(int from, int to, IntComparator comp, Swapper swapper) {
        int len = to - from;
        int m;
        int j;
        if (len < 16) {
            for (m = from; m < to; ++m) {
                for (j = m; j > from && comp.compare(j - 1, j) > 0; --j) {
                    swapper.swap(j, j - 1);
                }
            }
        } else {
            m = from + len / 2;
            j = from;
            int n = to - 1;
            int a;
            if (len > 128) {
                a = len / 8;
                j = med3(from, from + a, from + 2 * a, comp);
                m = med3(m - a, m, m + a, comp);
                n = med3(n - 2 * a, n - a, n, comp);
            }

            m = med3(j, m, n, comp);
            a = from;
            int b = from;
            int c = to - 1;
            int d = c;

            while (true) {
                int s;
                for (; b > c || (s = comp.compare(b, m)) > 0; swapper.swap(b++, c--)) {
                    for (; c >= b && (s = comp.compare(c, m)) >= 0; --c) {
                        if (s == 0) {
                            if (c == m) {
                                m = d;
                            } else if (d == m) {
                                m = c;
                            }

                            swapper.swap(c, d--);
                        }
                    }

                    if (b > c) {
                        s = Math.min(a - from, b - a);
                        swap(swapper, from, b - s, s);
                        s = Math.min(d - c, to - d - 1);
                        swap(swapper, b, to - s, s);
                        if ((s = b - a) > 1) {
                            quickSort(from, from + s, comp, swapper);
                        }

                        if ((s = d - c) > 1) {
                            quickSort(to - s, to, comp, swapper);
                        }

                        return;
                    }

                    if (b == m) {
                        m = d;
                    } else if (c == m) {
                        m = c;
                    }
                }

                if (s == 0) {
                    if (a == m) {
                        m = b;
                    } else if (b == m) {
                        m = a;
                    }

                    swapper.swap(a++, b);
                }

                ++b;
            }
        }
    }

    protected static void swap(Swapper swapper, int a, int b, int n) {
        for (int i = 0; i < n; ++b) {
            swapper.swap(a, b);
            ++i;
            ++a;
        }
    }

    private static int med3(int a, int b, int c, IntComparator comp) {
        int ab = comp.compare(a, b);
        int ac = comp.compare(a, c);
        int bc = comp.compare(b, c);
        return ab < 0 ? (bc < 0 ? b : (ac < 0 ? c : a)) : (bc > 0 ? b : (ac > 0 ? c : a));
    }
}
