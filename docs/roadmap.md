# Development Roadmap

## Phase 1: Foundation (Completed)
- [x] Database models (SQLAlchemy ORM)
- [x] Alembic migrations setup
- [x] Encryption service (Master Password + DEK)
- [x] Plugin architecture (base connector classes)
- [x] FastAPI app initialization
- [x] Connection CRUD endpoints

## Phase 2: Core Connectors (Completed)
- [x] PostgreSQL source/destination
- [x] SQLite source/destination
- [x] File connectors (CSV, Parquet, JSON)
- [x] S3 source/destination
- [x] MySQL source/destination
- [x] MSSQL source/destination
- [x] Oracle source/destination
- [x] Connection test endpoints (Refined)

## Phase 3: Pipeline Engine (Completed)
- [x] Celery worker setup
- [x] Pipeline execution engine (Source -> Processor -> Destination)
- [x] State management for incremental syncs
- [x] Scheduling with Celery Beat
- [x] Job progress tracking
- [x] WebSocket real-time updates

## Phase 4: Frontend
- [ ] React app initialization
- [ ] Connection manager UI
- [ ] Schema explorer with ERD (React Flow)
- [ ] Pipeline builder (drag-and-drop)
- [ ] Monitoring dashboard
- [ ] Log viewer

## Phase 5: Advanced Features
- [ ] MongoDB connector
- [ ] Snowflake connector
- [ ] BigQuery connector
- [ ] Data transformations (SQL layer with DuckDB)
- [ ] Alerting system (email/webhook on failure)
- [ ] Pipeline versioning

## Phase 6: SaaS Connectors (Future)
- [ ] Singer.io wrapper class
- [ ] Salesforce via tap-salesforce
- [ ] Stripe via tap-stripe
- [ ] Google Sheets via tap-google-sheets
