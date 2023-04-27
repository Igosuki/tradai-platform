use std::collections::HashSet;
use std::sync::Arc;
use std::time;

use actix::{Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, ResponseActFuture, Running,
            WrapFuture};
use backoff::ExponentialBackoff;
use time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

use brokers::types::MarketEventEnvelope;

use crate::driver::StrategyDriver;
use crate::query::{DataQuery, ModelReset, Mutation, StateFieldMutation};
use crate::{MarketChannel, StrategyLifecycleCmd, StrategyStatus};

#[derive(Clone, Debug, Deserialize)]
pub struct StrategyActorOptions {
    #[serde(deserialize_with = "util::ser::string_duration")]
    conn_backoff_max: Duration,
    #[serde(deserialize_with = "util::ser::string_duration")]
    order_resolution_interval: Duration,
}

impl Default for StrategyActorOptions {
    fn default() -> Self {
        Self {
            conn_backoff_max: Duration::from_secs(5),
            order_resolution_interval: Duration::from_secs(1),
        }
    }
}

pub type StrategySpawner = dyn Fn() -> Box<dyn StrategyDriver>;

pub struct StrategyActor {
    session_uuid: Uuid,
    spawner: Box<StrategySpawner>,
    inner: Arc<RwLock<Box<dyn StrategyDriver>>>,
    #[allow(dead_code)]
    conn_backoff: ExponentialBackoff,
    channels: HashSet<MarketChannel>,
    order_resolution_interval: Duration,
    is_checking_orders: bool,
}

impl StrategyActor {
    pub fn new(spawner: Box<StrategySpawner>, options: &StrategyActorOptions) -> Self {
        Self::new_with_uuid(spawner, options, Uuid::new_v4())
    }

    /// # Panics
    ///
    /// If the connection backoff cannot be parsed
    pub fn new_with_uuid(spawner: Box<StrategySpawner>, options: &StrategyActorOptions, session_uuid: Uuid) -> Self {
        let inner = spawner();
        let channels = inner.channels();
        let inner = Arc::new(RwLock::new(inner));
        Self {
            session_uuid,
            spawner,
            inner,
            channels,
            conn_backoff: ExponentialBackoff {
                max_elapsed_time: Some(options.conn_backoff_max),
                ..ExponentialBackoff::default()
            },
            order_resolution_interval: options.order_resolution_interval,
            is_checking_orders: false,
        }
    }

    pub(crate) fn channels(&self) -> HashSet<MarketChannel> { self.channels.clone() }
}

impl Actor for StrategyActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(uuid = %self.session_uuid, "strategy started");
        let inner = self.inner.clone();
        ctx.wait(
            async move {
                let mut w = inner.write().await;
                w.init().await
            }
            .into_actor(self)
            .map(|result, _, ctx| {
                if let Err(e) = result {
                    error!("failed to initialize strategy {}", e);
                    ctx.stop();
                }
            }),
        );
        ctx.run_interval(self.order_resolution_interval, |act, ctx| {
            // This will prevent stacking tasks if resolution interval is too low
            if act.is_checking_orders {
                return;
            }
            act.is_checking_orders = true;
            let inner = act.inner.clone();
            ctx.spawn(
                async move {
                    let mut w = inner.write().await;
                    w.resolve_orders().await;
                }
                .into_actor(act),
            );
            act.is_checking_orders = false;
        });
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {
        info!(uuid = %self.session_uuid, "strategy stopping, flushing...");
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!(uuid = %self.session_uuid, "strategy stopped");
    }
}

impl actix::Supervised for StrategyActor {
    fn restarting(&mut self, _ctx: &mut <Self as Actor>::Context) {
        info!(session_uuid = %self.session_uuid, "strategy restarting");
        self.inner = Arc::new(RwLock::new((self.spawner)()));
    }
}

type StratActorResponseFuture<T> = ResponseActFuture<StrategyActor, T>;

impl Handler<Arc<MarketEventEnvelope>> for StrategyActor {
    type Result = StratActorResponseFuture<anyhow::Result<()>>;

    #[cfg_attr(feature = "flame", flame)]
    fn handle(&mut self, msg: Arc<MarketEventEnvelope>, _ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut inner = lock.write().await;
                inner.on_market_event(msg.as_ref()).await.map_err(|e| anyhow!(e))
            }
            .into_actor(self),
        )
    }
}

impl Handler<DataQuery> for StrategyActor {
    type Result = StratActorResponseFuture<<DataQuery as actix::Message>::Result>;

    #[cfg_attr(feature = "flame", flame)]
    fn handle(&mut self, msg: DataQuery, _ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut inner = lock.write().await;
                Ok(inner
                    .query(msg)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })
                    .ok())
            }
            .into_actor(self),
        )
    }
}

impl Handler<StateFieldMutation> for StrategyActor {
    type Result = StratActorResponseFuture<<StateFieldMutation as actix::Message>::Result>;

    #[cfg_attr(feature = "flame", flame)]
    fn handle(&mut self, msg: StateFieldMutation, _ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut inner = lock.write().await;
                inner.mutate(Mutation::State(msg))
            }
            .into_actor(self),
        )
    }
}

impl Handler<ModelReset> for StrategyActor {
    type Result = StratActorResponseFuture<<ModelReset as actix::Message>::Result>;

    #[cfg_attr(feature = "flame", flame)]
    fn handle(&mut self, msg: ModelReset, _ctx: &mut Self::Context) -> Self::Result {
        let restart_after = msg.restart_after;
        let lock = self.inner.clone();
        Box::pin(
            async move {
                let mut inner = lock.write().await;
                if msg.stop_trading {
                    inner.stop_trading()?;
                }
                inner.mutate(Mutation::Model(msg))
            }
            .into_actor(self)
            .map(move |_, _act, ctx| {
                if restart_after {
                    ctx.stop();
                    Ok(StrategyStatus::Stopped)
                } else {
                    Ok(StrategyStatus::Running)
                }
            }),
        )
    }
}

impl Handler<StrategyLifecycleCmd> for StrategyActor {
    type Result = StratActorResponseFuture<<StrategyLifecycleCmd as actix::Message>::Result>;

    fn handle(&mut self, msg: StrategyLifecycleCmd, ctx: &mut Self::Context) -> Self::Result {
        let lock = self.inner.clone();
        match msg {
            StrategyLifecycleCmd::Restart => {
                ctx.stop();
                Box::pin(futures::future::ready(Ok(StrategyStatus::Running)).into_actor(self))
            }
            StrategyLifecycleCmd::StopTrading => Box::pin(
                async move {
                    let mut guard = lock.write().await;
                    guard.stop_trading()?;
                    Ok(StrategyStatus::NotTrading)
                }
                .into_actor(self),
            ),
            StrategyLifecycleCmd::ResumeTrading => Box::pin(
                async move {
                    let mut guard = lock.write().await;
                    guard.resume_trading()?;
                    Ok(StrategyStatus::Running)
                }
                .into_actor(self),
            ),
        }
    }
}

#[cfg(test)]
mod actor_test {
    use actix::{Actor, ActorContext, Context, Handler, Running, System};

    #[test]
    #[ignore]
    #[allow(clippy::items_after_statements)]
    fn test_actor() {
        util::test::init_test_env();

        #[derive(actix::Message)]
        #[rtype(result = "()")]
        enum Cmd {
            Restart,
            Stop,
        }
        struct A(&'static str);

        impl Actor for A {
            type Context = Context<Self>;

            fn started(&mut self, _: &mut Self::Context) {
                tracing::info!(name = %self.0, "actor started");
            }

            fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {
                tracing::info!(name = %self.0, "actor stopping...");
                Running::Stop
            }
        }

        impl actix::Supervised for A {
            fn restarting(&mut self, _ctx: &mut <Self as Actor>::Context) {
                tracing::info!(name = %self.0, "supervised restart");
            }
        }

        impl Handler<Cmd> for A {
            type Result = ();

            fn handle(&mut self, msg: Cmd, ctx: &mut Self::Context) -> Self::Result {
                if let Cmd::Restart = msg {
                    ctx.stop();
                }
                if let Cmd::Stop = msg {
                    ctx.stop();
                }
            }
        }
        let sleep = std::time::Duration::from_millis(200);
        System::new().block_on(async move {
            let _addr = A::start(A("unsupervised"));
            tokio::time::sleep(sleep).await;
            System::current().stop();
            tokio::time::sleep(sleep).await;
        });

        System::new().block_on(async move {
            let addr = A::start(A("unsupervised with cmd"));
            addr.send(Cmd::Restart).await.unwrap();
            System::current().stop();
            tokio::time::sleep(sleep).await;
        });

        System::new().block_on(async move {
            let _addr = actix::Supervisor::start(|_| A("supervised"));
            tokio::time::sleep(sleep).await;
            System::current().stop();
            tokio::time::sleep(sleep).await;
        });

        System::new().block_on(async move {
            let addr = actix::Supervisor::start(|_| A("supervised with cmd"));
            addr.send(Cmd::Restart).await.unwrap();
            addr.send(Cmd::Stop).await.unwrap();
            tokio::time::sleep(sleep).await;
            System::current().stop();
            tokio::time::sleep(sleep).await;
        });
    }
}
