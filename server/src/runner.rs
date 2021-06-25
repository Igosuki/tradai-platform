use crate::settings;
use crate::settings::Settings;
use actix::System;
#[cfg(feature = "gprof")]
use gperftools::heap_profiler::HEAP_PROFILER;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
#[cfg(feature = "flame_it")]
use std::fs::File;
use std::future::Future;
use std::process;
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct CliOptions {
    #[structopt(long)]
    check_conf: bool,
    #[structopt(short, long)]
    config: String,
    #[structopt(short, long)]
    telemetry: bool,
}

// TODO : clean up the ugly code for settings access
pub async fn with_config<F, T>(system_fn: F) -> std::io::Result<()>
where
    F: FnOnce(Arc<RwLock<Settings>>) -> T,
    T: Future<Output = std::io::Result<()>> + 'static,
{
    #[cfg(feature = "gprof")]
    HEAP_PROFILER.lock().unwrap().start("./trader.hprof").unwrap();

    let opts: CliOptions = CliOptions::from_args();
    let settings = Arc::new(RwLock::new(
        settings::Settings::new(opts.config).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
    ));

    // Create a channel to receive the events.
    let (tx, _rx) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx, std::time::Duration::from_secs(2)).unwrap();
    watcher
        .watch(&settings.read().unwrap().__config_file, RecursiveMode::NonRecursive)
        .unwrap();

    let settings_arc = Arc::clone(&settings);
    let settings_arc2 = settings_arc.clone();
    if opts.check_conf {
        settings_arc2.write().unwrap().sanitize();
        process::exit(0x0100);
    }
    if opts.telemetry {
        util::tracing::setup_opentelemetry();
    } else {
        env_logger::init();
    }
    let settings_guard = settings_arc2.read().unwrap();
    if settings_guard.profile_main {
        #[cfg(feature = "flame_it")]
        flame::start("main bot");
    }

    if let Err(e) = system_fn(settings_arc).await {
        error!("Trader system exited in error: {:?}", e);
    }
    if settings_guard.profile_main {
        #[cfg(feature = "gprof")]
        HEAP_PROFILER.lock().unwrap().stop().unwrap();
    }
    System::current().stop();
    info!("Caught interrupt and stopped the system");

    if settings_guard.profile_main {
        #[cfg(feature = "flame_it")]
        flame::end("main bot");

        #[cfg(feature = "flame_it")]
        flame::dump_html(&mut File::create("flame-graph.html").unwrap()).unwrap();
    }
    Ok(())
}
