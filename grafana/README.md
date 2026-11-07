# Grafana

`docker compose up` provisions a ClickHouse datasource automatically. Grafana
listens on 127.0.0.1 only and has no anonymous access: set
`GRAFANA_ADMIN_PASSWORD` before `docker compose up`, open
http://localhost:3000, log in as `admin`, and build a panel over the `rollups`
table, for example:

```sql
SELECT window_start_ms AS t, tenant, sum(count) AS events
FROM rollups FINAL
GROUP BY t, tenant
ORDER BY t
```

Suggested panels:
- Events per window per tenant (time series)
- Revenue (sum where kind = 'purchase') per window
- Refund rate: sum(kind='refund') / sum(kind='purchase')
