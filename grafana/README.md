# Grafana

`docker compose up` provisions a ClickHouse datasource automatically (anonymous
admin auth, dev only). Open http://localhost:3000 and build a panel over the
`rollups` table, for example:

```sql
SELECT window_start_ms AS t, tenant, sum(count) AS events
FROM rollups
GROUP BY t, tenant
ORDER BY t
```

Suggested panels:
- Events per window per tenant (time series)
- Revenue (sum where kind = 'purchase') per window
- Refund rate: sum(kind='refund') / sum(kind='purchase')
