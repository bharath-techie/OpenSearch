/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use crate::executor::DedicatedExecutor;
use crate::io::register_io_runtime;
use log::info;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tokio_metrics::RuntimeMonitor;

// RuntimeManager — owns IO runtime + CPU DedicatedExecutor.
pub struct RuntimeManager {
    pub io_runtime: Arc<Runtime>,
    pub cpu_executor: DedicatedExecutor,
    pub io_monitor: RuntimeMonitor,
    pub cpu_monitor: Option<RuntimeMonitor>,
}

impl RuntimeManager {
    pub fn new(cpu_threads: usize, datanode_multiplier: f64, _coordinator_multiplier: f64) -> Self {
        // dial9: start the telemetry session (no-op unless built --features dial9
        // on Linux AND DIAL9_ENABLED=1). Must run before any runtime is built so
        // dial9 can install its hooks as each runtime is constructed.
        crate::dial9_support::init();

        let io_threads = cpu_threads * 2;

        let mut io_builder = Builder::new_multi_thread();
        io_builder.worker_threads(io_threads).thread_name("datafusion-io").enable_all();
        // IO runtime has no per-worker thread-local to register, so on_start is a no-op.
        let io_runtime = Arc::new(crate::dial9_support::build_runtime("io", io_builder, || {}));

        register_io_runtime(Some(io_runtime.handle().clone()));

        // Install the global tracing dispatcher ONCE. Two optional layers can be
        // composed here (each gated by a cargo feature):
        //   * `tokio-console`  -> console-subscriber gRPC server (runtime task view)
        //   * `tracing-otel`   -> OTLP exporter -> Jaeger (span waterfall, browser UI)
        // Both can be on at once; we build ONE registry and set it ONCE, because
        // only a single global default dispatcher may be installed per process.
        //
        // IMPORTANT: this runs INSIDE the IO runtime's context (`_enter()`).
        // The OTLP/tonic exporter grabs the current Tokio reactor at construction
        // time; `RuntimeManager::new` is called from a plain JVM/JNI thread with no
        // ambient runtime, so building the exporter there panics with "there is no
        // reactor running". Entering the IO runtime first gives it a reactor.
        //
        // NOTE: we use `set_global_default`, NOT tracing-subscriber's `.init()`.
        // `.init()` also installs a LogTracer via `log::set_logger()`, but the
        // plugin already installs a global `log` logger (RustLoggerBridge), so a
        // second `set_logger` errors and `.init()` would panic. `set_global_default`
        // touches only the tracing dispatcher and leaves the `log` logger intact.
        #[cfg(any(feature = "tokio-console", feature = "tracing-otel"))]
        {
            use std::sync::Once;
            static TRACING_INIT: Once = Once::new();
            TRACING_INIT.call_once(|| {
                let _io_guard = io_runtime.enter();
                init_tracing_subscriber();
            });
        }

        let io_monitor = RuntimeMonitor::new(&io_runtime.handle());

        let io_handle = io_runtime.handle().clone();
        let mut cpu_runtime_builder = Builder::new_multi_thread();
        cpu_runtime_builder
            .worker_threads(cpu_threads)
            .thread_name("datafusion-cpu")
            // Poll the global/injection queue more often than the auto-tuned default
            // (~10ms-targeted). Top-level query/fragment futures are injected from
            // outside the CPU runtime (Java/IO thread -> global queue), so a tighter
            // interval lets idle workers pick them up sooner at the cost of slightly
            // more global-queue (mutex) synchronization. Local FIFO order is still
            // prioritized on the other (interval-1) ticks.
            .global_queue_interval(4)
            .enable_all()
            .on_thread_start(move || {
                register_io_runtime(Some(io_handle.clone()));
            });

        // Fragment executor concurrency gate: limits concurrent partition tasks from shard scans.
        let datanode_max_concurrent = (cpu_threads as f64 * datanode_multiplier).max(1.0) as usize;
        let cpu_executor = DedicatedExecutor::new("datafusion-cpu", cpu_runtime_builder, datanode_max_concurrent);

        let cpu_monitor = cpu_executor
            .handle()
            .map(|h| RuntimeMonitor::new(&h));

        Self {
            io_runtime,
            cpu_executor,
            io_monitor,
            cpu_monitor,
        }
    }

    pub fn cpu_executor(&self) -> DedicatedExecutor {
        self.cpu_executor.clone()
    }

    pub fn shutdown(&self) {
        info!("Shutting down RuntimeManager");
        self.cpu_executor.join_blocking();
        // dial9: flush + close the telemetry session so the trace is complete.
        crate::dial9_support::shutdown();
    }
}

impl Drop for RuntimeManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Build the global tracing dispatcher from whichever observability layers are
/// compiled in, and install it once. Called from `RuntimeManager::new` behind a
/// `Once`. No-op body unless `tokio-console` and/or `tracing-otel` are enabled.
#[cfg(any(feature = "tokio-console", feature = "tracing-otel"))]
fn init_tracing_subscriber() {
    use tracing_subscriber::prelude::*;

    // Start from an empty registry and conditionally attach each layer. Using
    // `.with(Option<Layer>)` lets a feature contribute `None` so the layer stack
    // type stays consistent regardless of which features are on.
    let registry = tracing_subscriber::registry();

    // --- tokio-console layer (runtime task view over gRPC :6669) ---
    #[cfg(feature = "tokio-console")]
    let registry = registry.with(
        console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .spawn(),
    );

    // --- OpenTelemetry/OTLP layer (span waterfall, exported to Jaeger) ---
    // Exports to the OTLP gRPC endpoint in OTEL_EXPORTER_OTLP_ENDPOINT
    // (default http://localhost:4317). View at Jaeger's UI (http://localhost:16686).
    //
    // Per-layer EnvFilter: WITHOUT this, the OTLP exporter's own gRPC client
    // (tonic/hyper/h2) emits spans — send_data, queue_frame, reserve_capacity,
    // hpack::encode — which flood Jaeger and bury the real query spans. We drop
    // those transport crates and default everything else to info, so only our
    // execute_query/execute_indexed (and DataFusion) spans are exported.
    // Override via RUST_LOG if set.
    #[cfg(feature = "tracing-otel")]
    let registry = {
        use tracing_subscriber::filter::EnvFilter;
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new(
                "info,h2=off,hyper=off,hyper_util=off,tonic=off,tower=off,\
                 opentelemetry=off,opentelemetry_sdk=off,opentelemetry_otlp=off,reqwest=off",
            )
        });
        match build_otel_layer() {
            Ok(layer) => {
                log::info!("tracing-otel: OTLP exporter initialized (Jaeger UI: http://localhost:16686)");
                registry.with(Some(layer.with_filter(filter)))
            }
            Err(e) => {
                log::warn!("tracing-otel: OTLP exporter init failed, spans disabled: {e}");
                registry.with(None)
            }
        }
    };

    match tracing::subscriber::set_global_default(registry) {
        Ok(()) => {
            #[cfg(feature = "tokio-console")]
            log::info!("tokio-console subscriber initialized (default 127.0.0.1:6669)");
        }
        Err(e) => log::warn!("tracing subscriber init skipped: {e}"),
    }
}

/// Construct the OpenTelemetry tracing layer backed by an OTLP/gRPC exporter.
/// Service name is "opensearch-datafusion"; endpoint comes from
/// OTEL_EXPORTER_OTLP_ENDPOINT (default http://localhost:4317).
///
/// Generic over the subscriber `S` it will be layered onto: when other layers
/// (e.g. tokio-console) are stacked first, `S` is the `Layered<...>` of those,
/// not the bare `Registry`. The `S: Subscriber + for<'a> LookupSpan<'a>` bounds
/// are what `OpenTelemetryLayer` requires to attach span context.
#[cfg(feature = "tracing-otel")]
fn build_otel_layer<S>() -> Result<
    tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>,
    Box<dyn std::error::Error>,
>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;

    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_attributes([KeyValue::new("service.name", "opensearch-datafusion")])
                .build(),
        )
        .build();

    let tracer = provider.tracer("opensearch-datafusion");
    // Keep the provider alive for the process lifetime so the batch exporter
    // keeps flushing. The RuntimeManager itself is process-lifetime, but the
    // provider is owned by OTel's global; set it so shutdown can flush.
    opentelemetry::global::set_tracer_provider(provider);

    Ok(tracing_opentelemetry::layer().with_tracer(tracer))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_mgr() -> RuntimeManager {
        RuntimeManager::new(1, 1.5, 1.5)
    }

    #[tokio::test]
    async fn test_runtime_manager_creates_and_shuts_down() {
        let mgr = test_mgr();
        let result = mgr.io_runtime.spawn(async { 42 }).await.unwrap();
        assert_eq!(result, 42);
        let result = mgr.cpu_executor().spawn(async { 99 }).await.unwrap();
        assert_eq!(result, 99);
        mgr.cpu_executor.shutdown();
        // Forget to avoid Drop which can't run in async context
        std::mem::forget(mgr);
    }

    #[tokio::test]
    async fn test_cpu_executor_runs_on_different_thread() {
        let mgr = test_mgr();
        let io_id = std::thread::current().id();
        let cpu_id = mgr
            .cpu_executor()
            .spawn(async { std::thread::current().id() })
            .await
            .unwrap();
        assert_ne!(io_id, cpu_id);
        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }

    #[tokio::test]
    async fn test_io_runtime_registered_on_cpu_threads() {
        let mgr = test_mgr();
        let has_io = mgr
            .cpu_executor()
            .spawn(async { crate::io::IO_RUNTIME.with_borrow(|h| h.is_some()) })
            .await
            .unwrap();
        assert!(has_io);
        mgr.cpu_executor.shutdown();
        std::mem::forget(mgr);
    }
}
