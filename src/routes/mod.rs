pub mod auth;
pub mod csv_proxy;
pub mod daily_hours;
pub mod drivers;
pub mod operations;
pub mod upload;
pub mod vehicles;

use axum::{middleware as axum_middleware, Router};

use crate::middleware::auth::require_jwt;
use crate::AppState;

pub fn router() -> Router<AppState> {
    let jwt_protected = Router::new()
        .merge(auth::protected_router())
        .merge(upload::router())
        .merge(operations::router())
        .merge(csv_proxy::router())
        .merge(drivers::router())
        .merge(vehicles::router())
        .merge(daily_hours::router())
        .layer(axum_middleware::from_fn(require_jwt));

    let public_routes = Router::new().merge(auth::public_router());

    Router::new()
        .merge(public_routes)
        .merge(jwt_protected)
}
