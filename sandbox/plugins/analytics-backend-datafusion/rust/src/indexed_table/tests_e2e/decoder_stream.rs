/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adaptive_decoder_matches_legacy_across_skipped_row_groups() {
    // Four 4-row groups. These collectors respectively leave an empty prefix,
    // empty middle groups, and an empty suffix around decoded row groups.
    for tag in [3, 4, 5] {
        let tree = BoolNode::And(vec![index_leaf(tag)]);
        let legacy = run_tree_with_decoder(tree.clone(), false).await;
        let adaptive = run_tree_with_decoder(tree, true).await;
        assert_eq!(adaptive, legacy, "decoder mismatch for collector tag {tag}");
        assert!(!adaptive.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adaptive_decoder_records_elapsed_compute() {
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (_, plan) = run_tree_and_plan_with_decoder(tree, true).await;
    let metrics = super::metrics::aggregate_metrics(&plan);
    let elapsed_ns = metrics
        .sum(|metric| metric.value().name() == "elapsed_compute")
        .map(|value| value.as_usize())
        .unwrap_or(0);
    assert!(
        elapsed_ns > 0,
        "adaptive decoder elapsed_compute should be nonzero"
    );
}
