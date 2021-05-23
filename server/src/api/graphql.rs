/*!

# juniper_actix

This repository contains the [actix][actix] web server integration for
[Juniper][Juniper], a [GraphQL][GraphQL] implementation for Rust, its inspired and some parts are copied from [juniper_warp][juniper_warp]

## Documentation

For documentation, including guides and examples, check out [Juniper][Juniper].

A basic usage example can also be found in the [API documentation][documentation].

## Examples

Check [examples/actix_server][example] for example code of a working actix
server with GraphQL handlers.

## Links

* [Juniper][Juniper]
* [API Reference][documentation]
* [actix][actix]

## License

This project is under the BSD-2 license.

Check the LICENSE file for details.

[actix]: https://github.com/actix/actix-web
[Juniper]: https://github.com/graphql-rust/juniper
[GraphQL]: http://graphql.org
[documentation]: https://docs.rs/juniper_actix
[example]: https://github.com/graphql-rust/juniper/blob/master/juniper_actix/examples/actix_server.rs
[juniper_warp]: https://github.com/graphql-rust/juniper/juniper_warp
*/

#![deny(missing_docs)]
#![deny(warnings)]

// use futures::{FutureExt as _};
use super::playground_source::playground_source;
use actix_web::{error::{ErrorBadRequest, ErrorMethodNotAllowed, ErrorUnsupportedMediaType}, http::Method, web, Error, FromRequest, HttpRequest, HttpResponse, HttpMessage};
use juniper::{
    http::{graphiql::graphiql_source, GraphQLBatchRequest, GraphQLRequest},
    ScalarValue,
};
use serde::Deserialize;

#[derive(Deserialize, Clone, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
struct GetGraphQLRequest {
    query: String,
    #[serde(rename = "operationName")]
    operation_name: Option<String>,
    variables: Option<String>,
}

impl<S> From<GetGraphQLRequest> for GraphQLRequest<S>
where
    S: ScalarValue,
{
    fn from(get_req: GetGraphQLRequest) -> Self {
        let GetGraphQLRequest {
            query,
            operation_name,
            variables,
        } = get_req;
        let variables = match variables {
            Some(variables) => Some(serde_json::from_str(&variables).unwrap()),
            None => None,
        };
        Self::new(query, operation_name, variables)
    }
}

/// Actix Web GraphQL Handler for GET and POST requests
pub async fn graphql_handler<Query, Mutation, Subscription, Context, S>(
    schema: &juniper::RootNode<'static, Query, Mutation, Subscription, S>,
    context: &Context,
    req: HttpRequest,
    payload: actix_web::web::Payload,
) -> Result<HttpResponse, Error>
where
    S: ScalarValue + Send + Sync + 'static,
    Context: Send + Sync + 'static,
    Query: juniper::GraphQLTypeAsync<S, Context = Context> + Send + Sync + 'static,
    Query::TypeInfo: Send + Sync,
    Mutation: juniper::GraphQLTypeAsync<S, Context = Context> + Send + Sync + 'static,
    Mutation::TypeInfo: Send + Sync,
    Subscription: juniper::GraphQLSubscriptionType<S, Context = Context> + Send + Sync + 'static,
    Subscription::TypeInfo: Send + Sync,
{
    match *req.method() {
        Method::POST => post_graphql_handler(schema, context, req, payload).await,
        Method::GET => get_graphql_handler(schema, context, req).await,
        _ => Err(ErrorMethodNotAllowed(
            "GraphQL requests can only be sent with GET or POST",
        )),
    }
}
/// Actix GraphQL Handler for GET requests
pub async fn get_graphql_handler<Query, Mutation, Subscription, Context, S>(
    schema: &juniper::RootNode<'static, Query, Mutation, Subscription, S>,
    context: &Context,
    req: HttpRequest,
) -> Result<HttpResponse, Error>
where
    S: ScalarValue + Send + Sync + 'static,
    Context: Send + Sync + 'static,
    Query: juniper::GraphQLTypeAsync<S, Context = Context> + Send + Sync + 'static,
    Query::TypeInfo: Send + Sync,
    Mutation: juniper::GraphQLTypeAsync<S, Context = Context> + Send + Sync + 'static,
    Mutation::TypeInfo: Send + Sync,
    Subscription: juniper::GraphQLSubscriptionType<S, Context = Context> + Send + Sync + 'static,
    Subscription::TypeInfo: Send + Sync,
{
    let get_req = web::Query::<GetGraphQLRequest>::from_query(req.query_string())?;
    let req = GraphQLRequest::from(get_req.into_inner());
    let gql_response = req.execute(schema, context).await;
    let body_response = serde_json::to_string(&gql_response).map_err(|_e| actix_web::web::HttpResponse::internal_server_error())?;
    let mut response = match gql_response.is_ok() {
        true => HttpResponse::Ok(),
        false => HttpResponse::BadRequest(),
    };
    Ok(response
        .content_type("application/json")
        .body(body_response))
}

/// Actix GraphQL Handler for POST requests
pub async fn post_graphql_handler<Query, Mutation, Subscription, Context, S>(
    schema: &juniper::RootNode<'static, Query, Mutation, Subscription, S>,
    context: &Context,
    req: HttpRequest,
    payload: actix_web::web::Payload,
) -> Result<HttpResponse, Error>
where
    S: ScalarValue + Send + Sync + 'static,
    Context: Send + Sync + 'static,
    Query: juniper::GraphQLTypeAsync<S, Context = Context> + Send + Sync + 'static,
    Query::TypeInfo: Send + Sync,
    Mutation: juniper::GraphQLTypeAsync<S, Context = Context> + Send + Sync + 'static,
    Mutation::TypeInfo: Send + Sync,
    Subscription: juniper::GraphQLSubscriptionType<S, Context = Context> + Send + Sync + 'static,
    Subscription::TypeInfo: Send + Sync,
{
    let req = match req.content_type() {
        "application/json" => {
            let body = String::from_request(&req, &mut payload.into_inner()).await?;
            serde_json::from_str::<GraphQLBatchRequest<S>>(&body).map_err(ErrorBadRequest)
        }
        "application/graphql" => {
            let body = String::from_request(&req, &mut payload.into_inner()).await?;
            Ok(GraphQLBatchRequest::Single(GraphQLRequest::new(
                body, None, None,
            )))
        }
        _ => Err(ErrorUnsupportedMediaType(
            "GraphQL requests should have content type `application/json` or `application/graphql`",
        )),
    }?;
    let gql_batch_response = req.execute(schema, context).await;
    let gql_response = serde_json::to_string(&gql_batch_response)
        .map_err(|_e| actix_web::web::HttpResponse::internal_server_error())?;
    let mut response = match gql_batch_response.is_ok() {
        true => HttpResponse::Ok(),
        false => HttpResponse::BadRequest(),
    };
    Ok(response.content_type("application/json").body(gql_response))
}

/// Create a handler that replies with an HTML page containing GraphiQL. This does not handle routing, so you can mount it on any endpoint
///
/// For example:
///
/// ```
/// # extern crate actix;
/// # extern crate juniper_actix;
/// #
/// # use juniper_actix::graphiql_handler;
/// # use actix_web::{web, App};
///
/// let app = App::new()
///          .route("/", web::get().to(|| graphiql_handler("/graphql", Some("/graphql/subscriptions"))));
/// ```
#[allow(dead_code)]
pub async fn graphiql_handler(
    graphql_endpoint_url: &str,
    subscriptions_endpoint_url: Option<&'static str>,
) -> Result<HttpResponse, Error> {
    let html = graphiql_source(graphql_endpoint_url, subscriptions_endpoint_url);
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}

/// Create a handler that replies with an HTML page containing GraphQL Playground. This does not handle routing, so you cant mount it on any endpoint.
pub async fn playground_handler(
    graphql_endpoint_url: &str,
    subscriptions_endpoint_url: Option<&'static str>,
) -> Result<HttpResponse, Error> {
    let html = playground_source(graphql_endpoint_url, subscriptions_endpoint_url);
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}
