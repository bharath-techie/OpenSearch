/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.RowJsonReader;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes a composite merge: primary format first, then secondaries using the
 * row-ID mapping from the primary. Stateless — all state comes from the
 * {@link MergePlan} and the merger map.
 *
 * <h2>Materialized-view mode</h2>
 *
 * <p>For MV indices the primary's merge is an aggregating fold: N input rows collapse
 * into M result rows, which are <em>new</em> rows carrying new row IDs {@code 0..M-1}.
 * Instead of receiving a row-ID mapping (none exists — the fold severs per-row
 * provenance), each secondary is <em>rebuilt</em>: the folded primary rows are read
 * back and re-driven through the document mapper into a replacement segment at the
 * merged generation, which the secondary's merger swaps in while consuming the input
 * segments. Every format therefore holds exactly the same M rows and the cross-format
 * row-count invariant holds unconditionally.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeMergeExecutor {

    private static final Logger logger = LogManager.getLogger(CompositeMergeExecutor.class);

    /**
     * Context for materialized-view merges: how to read the primary's folded output and
     * which engines to rebuild secondaries with.
     *
     * @param primaryRowReader reads folded primary rows back as JSON
     * @param secondaryEngines secondary engines keyed by format
     * @param rebuilder        drives folded rows through the mapper into replacement segments
     */
    @ExperimentalApi
    public record MvRebuildContext(
        RowJsonReader primaryRowReader,
        Map<DataFormat, IndexingExecutionEngine<?, ?>> secondaryEngines,
        MvSecondaryRebuilder rebuilder
    ) {}

    private final Map<DataFormat, Merger> mergers;
    private final MvRebuildContext mvContext;

    public CompositeMergeExecutor(Map<DataFormat, Merger> mergers) {
        this(mergers, null);
    }

    public CompositeMergeExecutor(Map<DataFormat, Merger> mergers, MvRebuildContext mvContext) {
        this.mergers = Map.copyOf(mergers);
        this.mvContext = mvContext;
    }

    /**
     * Executes the merge described by the plan.
     *
     * @param plan the pre-validated merge plan
     * @return the combined merge result across all formats
     */
    public MergeResult execute(MergePlan plan) {
        boolean mvMode = mvContext != null;
        List<FormatMergeResult> completed = new ArrayList<>();
        List<WriterFileSet> replacements = new ArrayList<>();
        try {
            FormatMergeResult primaryResult = mergeFormat(plan, plan.primaryFormat(), null, null);
            completed.add(primaryResult);

            RowIdMapping mapping = plan.hasSecondaries() && mvMode == false
                ? primaryResult.rowIdMappingOpt()
                    .orElseThrow(() -> new IllegalStateException("Primary merge did not produce row-ID mapping required by secondaries"))
                : null;

            String foldedRowsJson = null;
            for (DataFormat secondary : plan.secondaryFormats()) {
                WriterFileSet replacement = null;
                if (mvMode && primaryResult.mergedFiles() != null) {
                    if (foldedRowsJson == null) {
                        foldedRowsJson = mvContext.primaryRowReader().readRowsAsJson(primaryResult.mergedFiles());
                    }
                    IndexingExecutionEngine<?, ?> engine = mvContext.secondaryEngines().get(secondary);
                    if (engine == null) {
                        throw new IllegalStateException("MV merge: no engine registered for secondary format [" + secondary.name() + "]");
                    }
                    replacement = mvContext.rebuilder().rebuild(engine, foldedRowsJson, plan.mergedWriterGeneration());
                    replacements.add(replacement);
                }
                FormatMergeResult secondaryResult = mergeFormat(plan, secondary, mapping, replacement);
                // Verify secondary produced output when primary did
                if (primaryResult.mergedFiles() != null && secondaryResult.mergedFiles() == null) {
                    throw new IllegalStateException(
                        "Primary format ["
                            + plan.primaryFormat().name()
                            + "] produced merged output but secondary format ["
                            + secondary.name()
                            + "] returned null — possible concurrent merge consumed segments"
                    );
                }
                // Verify secondary merged row count matches primary — this invariant holds
                // unconditionally: in MV mode the secondary was rebuilt 1:1 from the folded
                // primary output.
                if (primaryResult.mergedFiles() != null && secondaryResult.mergedFiles() != null) {
                    long primaryRows = primaryResult.mergedFiles().numRows();
                    long secondaryRows = secondaryResult.mergedFiles().numRows();
                    if (primaryRows != secondaryRows) {
                        throw new IllegalStateException(
                            "Row count mismatch after merge: primary format ["
                                + plan.primaryFormat().name()
                                + "] has "
                                + primaryRows
                                + " rows but secondary format ["
                                + secondary.name()
                                + "] has "
                                + secondaryRows
                                + " rows"
                        );
                    }
                }
                completed.add(secondaryResult);
            }

            return toMergeResult(completed, mapping, mvMode);
        } catch (Exception e) {
            completed.forEach(FormatMergeResult::cleanup);
            if (e instanceof RuntimeException re) throw re;
            throw new UncheckedIOException((IOException) e);
        } finally {
            // Replacement segments are copied into each secondary's merged output by its
            // merger; the temp flush directories are no longer needed either way.
            for (WriterFileSet replacement : replacements) {
                try {
                    IOUtils.rm(Path.of(replacement.directory()));
                } catch (IOException e) {
                    logger.warn("Failed to remove MV rebuild temp directory [{}]", replacement.directory(), e);
                }
            }
        }
    }

    private FormatMergeResult mergeFormat(MergePlan plan, DataFormat format, RowIdMapping mapping, WriterFileSet replacement)
        throws IOException {
        Merger merger = mergers.get(format);
        List<WriterFileSet> files = plan.filesFor(format);
        List<Segment> segments = new ArrayList<>();
        for (WriterFileSet wfs : files) {
            segments.add(Segment.builder(wfs.writerGeneration()).addSearchableFiles(format, wfs).build());
        }
        MergeResult result = merger.merge(new MergeInput(segments, mapping, plan.mergedWriterGeneration(), replacement));
        return new FormatMergeResult(format, result.getMergedWriterFileSetForDataformat(format), result.rowIdMapping().orElse(null));
    }

    private static MergeResult toMergeResult(List<FormatMergeResult> results, RowIdMapping mapping, boolean aggregating) {
        Map<DataFormat, WriterFileSet> merged = new HashMap<>();
        for (FormatMergeResult r : results) {
            merged.put(r.format(), r.mergedFiles());
        }
        return new MergeResult(merged, mapping, aggregating);
    }
}
