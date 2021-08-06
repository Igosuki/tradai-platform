use std::sync::Arc;
use std::time;

use actix::prelude::*;
use actix::{Actor, AsyncContext, Context};
use awc::Client;
use prometheus::Counter;

pub mod push;

lazy_static! {
    static ref PUSH_COUNTER: Counter =
        register_counter!("push_total", "Total number of prometheus client pushed.").unwrap();
    static ref PUSH_ERROR: Counter =
        register_counter!("push_errors", "Total number of prometheus client push errors.").unwrap();
    static ref PUSH_SUCCESS: Counter =
        register_counter!("push_success", "Total number of prometheus client successful pushes.").unwrap();
}

#[derive(Debug, Deserialize)]
pub struct PrometheusOptions {
    pub push_gateway: String,
    pub instance: String,
}

pub struct PrometheusPushActor {
    push_frequency: time::Duration,
    client: Arc<Client>,
    address: String,
    instance_name: String,
}
pub async fn push_metrics(c: Arc<Client>, address: Arc<String>, instance: Arc<String>) {
    PUSH_COUNTER.inc();
    let metric_families = prometheus::gather();
    let instance_name = instance.clone();
    match push::push_metrics(
        &c,
        "pushgateway",
        labels! {"instance".to_owned() => instance_name.to_string()},
        &address,
        metric_families,
        None,
    )
    .await
    {
        Ok(_) => PUSH_SUCCESS.inc(),
        Err(_) => PUSH_ERROR.inc(),
    }
}
impl PrometheusPushActor {
    pub fn new(options: &PrometheusOptions) -> Self {
        PrometheusPushActor {
            push_frequency: time::Duration::from_secs(5),
            client: Arc::new(push::make_client()),
            address: options.push_gateway.to_string(),
            instance_name: options.instance.to_string(),
        }
    }
}

impl Actor for PrometheusPushActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let ca = self.client.clone();
        let push_address = Arc::new(self.address.clone());
        let instance_name = Arc::new(self.instance_name.clone());

        ctx.run_interval(self.push_frequency, move |a, c| {
            c.spawn(push_metrics(ca.clone(), push_address.clone(), instance_name.clone()).into_actor(a));
        });
        info!("prometheus push started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("prometheus push stopped");
    }
}
