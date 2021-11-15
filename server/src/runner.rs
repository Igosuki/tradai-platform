#[cfg(feature = "flame_it")]
use std::fs::File;
use std::future::Future;
use std::process;
use std::sync::mpsc::channel;
use std::sync::Arc;

use actix::System;
#[cfg(feature = "gprof")]
use gperftools::heap_profiler::HEAP_PROFILER;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use structopt::StructOpt;
use tokio::sync::RwLock;

use crate::settings;
use crate::settings::{Settings, Version};

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct CliOptions {
    #[structopt(long)]
    check_conf: bool,
    #[structopt(short, long)]
    config: Option<String>,
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
    let config_file = get_config_file(&opts).await;
    let settings = Arc::new(RwLock::new(
        settings::Settings::new(config_file).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
    ));

    settings.write().await.version = Some(Version {
        version: env!("CARGO_PKG_VERSION").to_string(),
        sha: env!("GIT_HASH").to_string(),
    });

    // Create a channel to receive the events.
    let (tx, _rx) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx, std::time::Duration::from_secs(2)).unwrap();
    let settings_r = settings.read().await;
    watcher
        .watch(&settings_r.__config_file, RecursiveMode::NonRecursive)
        .unwrap();

    if opts.check_conf {
        settings_r.sanitize();
        process::exit(0x0100);
    }
    if opts.telemetry {
        let telemetry = &settings_r.telemetry;
        util::tracing::setup_opentelemetry(
            telemetry.agents.clone(),
            telemetry.service_name.clone(),
            telemetry.tags.clone(),
        );
    } else {
        env_logger::init();
    }

    if settings_r.profile_main {
        #[cfg(feature = "flame_it")]
        flame::start("main bot");
    }

    if let Err(e) = system_fn(settings.clone()).await {
        error!("Trader system exited in error: {:?}", e);
    }
    if settings_r.profile_main {
        #[cfg(feature = "gprof")]
        HEAP_PROFILER.lock().unwrap().stop().unwrap();
    }
    System::current().stop();
    info!("Caught interrupt and stopped the system");

    if settings_r.profile_main {
        #[cfg(feature = "flame_it")]
        flame::end("main bot");

        #[cfg(feature = "flame_it")]
        flame::dump_html(&mut File::create("flame-graph.html").unwrap()).unwrap();
    }
    Ok(())
}

async fn get_config_file(opts: &CliOptions) -> String {
    let config_input = opts.config.as_deref().unwrap_or("./config");
    let config_metadata = std::fs::metadata(&config_input).unwrap_or_else(|_| {
        panic!(
            "{}",
            format!("missing configuration file or directory {}", config_input)
        )
    });
    let config_file = if config_metadata.is_file() {
        config_input.to_string()
    } else {
        #[cfg(not(feature = "dialoguer"))]
        panic!("dialoguer is required to pick a configuration from the terminal");
        #[cfg(feature = "dialoguer")]
        config_from_stdin(&config_input).await
    };
    config_file
}

#[cfg(feature = "dialoguer")]
async fn config_from_stdin(config_input: &&str) -> String {
    use std::path::PathBuf;
    let mut p = PathBuf::from(&config_input);
    p.push("*.yaml");
    let glob_pattern = p.to_str().unwrap();
    let glob_r = glob::glob(glob_pattern);
    let files = glob_r.unwrap_or_else(|_| panic!("{}", format!("invalid glob {:?}", p)));
    let mut choices = vec![];
    for file in files {
        choices.push(file.unwrap().to_string_lossy().to_string());
    }
    let path = tokio::time::timeout(std::time::Duration::from_secs(30), async move {
        let selection = prompt_choice(&mut choices);
        choices[selection].clone()
    })
    .await
    .unwrap();
    path
}

#[cfg(feature = "dialoguer")]
fn prompt_choice(choices: &mut Vec<String>) -> usize {
    use dialoguer::console::Style;
    let mut prompt_theme = dialoguer::theme::ColorfulTheme::default();
    let white = Style::new().for_stdout().white();
    prompt_theme.hint_style = white;
    prompt_theme.prompt_suffix = prompt_theme.prompt_suffix.white();
    let term = dialoguer::console::Term::buffered_stdout();
    let selection = dialoguer::Select::with_theme(&prompt_theme)
        .with_prompt("Pick a configuration")
        .default(0)
        .items(&choices[..])
        .interact_on(&term)
        .unwrap();
    selection
}
