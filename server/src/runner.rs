use crate::settings;
use crate::settings::{Settings, Version};
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
    #[structopt(short, long)]
    version: bool,
}

pub async fn with_config<F, T>(system_fn: F) -> anyhow::Result<()>
where
    F: FnOnce(Arc<RwLock<Settings>>) -> T,
    T: Future<Output = anyhow::Result<()>> + 'static,
{
    #[cfg(feature = "gprof")]
    HEAP_PROFILER.lock().unwrap().start("./trader.hprof").unwrap();

    let opts: CliOptions = CliOptions::from_args();
    if opts.version {
        println!("Build Version: {}", env!("CARGO_PKG_VERSION"));
        println!("Commit Hash: {}", env!("GIT_HASH"));
        process::exit(0x0100);
    }

    let settings = Arc::new(RwLock::new(
        settings::Settings::new(opts.config).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
    ));

    settings.write().unwrap().version = Some(Version {
        version: env!("CARGO_PKG_VERSION").to_string(),
        sha: env!("GIT_HASH").to_string(),
    });

    // Create a channel to receive the events.
    let (tx, _rx) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx, std::time::Duration::from_secs(2)).unwrap();
    watcher
        .watch(&settings.read().unwrap().__config_file, RecursiveMode::NonRecursive)
        .unwrap();

    if opts.check_conf {
        settings.read().unwrap().sanitize();
        process::exit(0x0100);
    }
    if opts.telemetry {
        let endpoints = settings.read().unwrap();
        let telemetry = &endpoints.telemetry;
        util::tracing::setup_opentelemetry(
            telemetry.agents.clone(),
            telemetry.service_name.clone(),
            telemetry.tags.clone(),
        );
    } else {
        env_logger::init();
    }

    let settings_guard = settings.read().unwrap();
    if settings_guard.profile_main {
        #[cfg(feature = "flame_it")]
        flame::start("main bot");
    }

    if let Err(e) = system_fn(settings.clone()).await {
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
