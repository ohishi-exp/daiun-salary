use sqlx::PgConnection;

pub async fn set_current_tenant(
    conn: &mut PgConnection,
    tenant_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!(
        "SET LOCAL app.current_tenant_id = '{}'",
        tenant_id
    ))
    .execute(conn)
    .await?;
    Ok(())
}
