# orders-airflow-dbt

Airflow DAG kechagi buyurtmalarni PostgreSQL (`shop.orders`) dan `warehouse.staging.orders` jadvaliga
yuklaydi (**idempotent**: shu kun qatorlari o'chirilib qayta yoziladi, bitta tranzaksiyada), so'ng dbt
`daily_revenue` modelini quradi va `unique` / `not_null` testlarini ishga tushiradi.

```
shop.orders --(Python task)--> warehouse.staging.orders --(dbt run)--> analytics.daily_revenue --(dbt test)
```

## Ishga tushirish

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

1. http://localhost:8080 oching (login: `admin` / `admin`).
2. `orders_daily_elt` DAG'ni yoqing va **Trigger** bosing.
3. Natijani tekshiring:

```bash
docker compose exec postgres psql -U airflow -d warehouse -c "select * from analytics.daily_revenue order by 1"
```

DAG'ni qayta-qayta ishga tushirsangiz ham `staging.orders` da dublikat bo'lmaydi.

## Tuzilma

- `dags/orders_daily_elt.py` — DAG (load -> dbt run -> dbt test)
- `dbt_project/` — dbt loyiha (`models/marts/daily_revenue.sql`, testlar `schema.yml` va `sources.yml` da)
- `initdb/01_init.sql` — `shop` va `warehouse` bazalari va test ma'lumotlar
- `docker-compose.yml`, `Dockerfile` — Airflow + Postgres + dbt (alohida venv da)
