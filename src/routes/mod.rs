pub mod api_tokens;
pub mod auth;
pub mod csv_proxy;
pub mod daily_hours;
pub mod drivers;
pub mod event_classifications;
pub mod gateway;
pub mod members;
pub mod operations;
pub mod restraint_report;
pub mod scraper;
pub mod upload;
pub mod vehicles;
pub mod work_times;

use axum::{middleware as axum_middleware, Router};

use crate::middleware::auth::{require_jwt, require_jwt_or_gateway};
use crate::AppState;

pub fn router() -> Router<AppState> {
    // Write endpoints: JWT only (admin users)
    let jwt_only = Router::new()
        .merge(auth::protected_router())
        .merge(upload::router())
        .merge(api_tokens::router())
        .merge(members::router())
        .merge(scraper::router())
        .layer(axum_middleware::from_fn(require_jwt));

    // Read endpoints: JWT or gateway secret (external API clients)
    let read_routes = Router::new()
        .merge(operations::router())
        .merge(csv_proxy::router())
        .merge(drivers::router())
        .merge(vehicles::router())
        .merge(daily_hours::router())
        .merge(event_classifications::router())
        .merge(work_times::router())
        .merge(restraint_report::router())
        .layer(axum_middleware::from_fn(require_jwt_or_gateway));

    // Public routes (no auth)
    let public_routes = Router::new()
        .merge(auth::public_router())
        .merge(gateway::router());

    Router::new()
        .merge(public_routes)
        .merge(jwt_only)
        .merge(read_routes)
}
