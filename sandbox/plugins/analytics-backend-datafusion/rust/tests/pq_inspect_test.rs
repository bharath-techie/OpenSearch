// Inspect the real clickbench parquet files: compression codec, page sizes, row
// group layout. Diagnostic only.
//
//   cargo test -p opensearch-datafusion --test pq_inspect_test -- --ignored --nocapture

use std::fs::File;
use std::sync::Arc;

use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

const DATA_DIR: &str =
    "/Users/gbh/Documents/data/nodes/0/indices/euv-GKLFSeSkrvwjOQZ9jQ/0/parquet";

fn meta(file: &str) -> Arc<ParquetMetaData> {
    let f = File::open(format!("{DATA_DIR}/{file}")).unwrap();
    let r = SerializedFileReader::new(f).unwrap();
    r.metadata().clone().into()
}

#[test]
#[ignore]
fn inspect() {
    for file in [
        "_parquet_file_generation_merged_433.parquet",
        "_parquet_file_generation_merged_117.parquet",
        "_parquet_file_generation_492.parquet",
    ] {
        let md = meta(file);
        println!(
            "\n=== {file}: {} row groups, {} rows",
            md.num_row_groups(),
            md.file_metadata().num_rows()
        );
        println!("writer: {:?}", md.file_metadata().created_by());
        // codecs across all columns of RG0
        let rg0 = md.row_group(0);
        let mut codecs = std::collections::BTreeMap::new();
        for c in 0..rg0.num_columns() {
            let col = rg0.column(c);
            *codecs.entry(format!("{:?}", col.compression())).or_insert(0) += 1;
        }
        println!("RG0 codecs: {codecs:?}  cols={}", rg0.num_columns());
        for rg in 0..md.num_row_groups().min(6) {
            let g = md.row_group(rg);
            println!(
                "  RG{rg}: rows={} bytes={} ",
                g.num_rows(),
                g.total_byte_size()
            );
        }
        // largest column chunks in RG0
        let mut sizes: Vec<(i64, String, String)> = (0..rg0.num_columns())
            .map(|c| {
                let col = rg0.column(c);
                (
                    col.compressed_size(),
                    col.column_path().string(),
                    format!("{:?}", col.compression()),
                )
            })
            .collect();
        sizes.sort_by_key(|(s, _, _)| -s);
        for (s, name, codec) in sizes.iter().take(5) {
            println!("    big col: {name} {codec} compressed={s}");
        }
    }
}

/// Walk every page header of a column chunk looking for an uncompressed size of
/// exactly 2 MiB (the size in the reported failure), and report the codec.
#[test]
#[ignore]
fn find_2mib_pages() {
    use datafusion::parquet::column::page::PageReader;

    let file = "_parquet_file_generation_merged_433.parquet";
    let f = File::open(format!("{DATA_DIR}/{file}")).unwrap();
    let reader = SerializedFileReader::new(f).unwrap();
    let md = reader.metadata();

    let mut found = 0;
    for rg in 0..md.num_row_groups().min(3) {
        let rg_reader = reader.get_row_group(rg).unwrap();
        let rgmd = md.row_group(rg);
        for c in 0..rgmd.num_columns() {
            let col = rgmd.column(c);
            let mut pages = match rg_reader.get_column_page_reader(c) {
                Ok(p) => p,
                Err(e) => {
                    println!(
                        "RG{rg} col{c} {} page reader ERROR: {e}",
                        col.column_path().string()
                    );
                    continue;
                }
            };
            loop {
                match pages.get_next_page() {
                    Ok(Some(page)) => {
                        let un = page.buffer().len();
                        if un >= 2 * 1024 * 1024 - 8 && un <= 2 * 1024 * 1024 + 8 {
                            found += 1;
                            if found < 10 {
                                println!(
                                    "RG{rg} col{c} {} {:?} page decoded_len={un}",
                                    col.column_path().string(),
                                    col.compression()
                                );
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        println!(
                            "RG{rg} col{c} {} {:?} DECODE ERROR: {e}",
                            col.column_path().string(),
                            col.compression()
                        );
                        break;
                    }
                }
            }
        }
    }
    println!("pages near 2MiB: {found}");
}
