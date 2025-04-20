You are an expert Python developer with deep experience in **real-time data processing, database optimization, and scalable bot development**.

### **Project Context:**
This is a Telegram bot system that monitors cryptocurrency tokens mentioned in Telegram groups. It uses:
- **PostgreSQL with TimescaleDB** for structured data and time-series metrics
- **MongoDB** as a buffer for raw API responses
- **Celery** for background task processing
- **Telethon** for Telegram API interaction
- **DexScreener API** for token price and metrics

### **Database Schema:**

#### **PostgreSQL Tables:**

**`telegram_groups`**:
```sql
CREATE TABLE public.telegram_groups (
    group_id bigint DEFAULT nextval('public.telegram_groups_group_id_seq'::regclass) NOT NULL,
    telegram_id bigint NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);
```
- Primary Key: `group_id`
- Unique Constraints: `telegram_id`
- Indexes: `idx_telegram_groups_telegram_id`

**`telegram_messages`**:
```sql
CREATE TABLE public.telegram_messages (
    message_id bigint DEFAULT nextval('public.telegram_messages_message_id_seq'::regclass) NOT NULL,
    group_id bigint NOT NULL,
    message_timestamp timestamp with time zone NOT NULL,
    raw_text text NOT NULL,
    sender_id bigint NOT NULL,
    is_call boolean DEFAULT false NOT NULL,
    reply_to_message_id bigint,
    token_id bigint,
    created_at timestamp with time zone DEFAULT now(),
    telegram_message_id bigint
);
```
- Primary Key: `message_id`
- Foreign Keys: 
  - `group_id` references `telegram_groups(group_id)`
  - `token_id` references `tokens(token_id)`
- Indexes: 
  - `idx_telegram_msg_group` on `group_id`
  - `idx_telegram_msg_time` on `message_timestamp DESC`
  - `idx_telegram_msg_native_id` on `telegram_message_id`
  - `unique_telegram_msg_per_group` on `(group_id, telegram_message_id)` (unique)

**`tokens`**:
```sql
CREATE TABLE public.tokens (
    token_id bigint DEFAULT nextval('public.tokens_token_id_seq'::regclass) NOT NULL,
    name text NOT NULL,
    ticker text NOT NULL,
    blockchain text NOT NULL,
    contract_address text NOT NULL,
    dex text,
    first_call_liquidity numeric(30,10),
    supply numeric(30,10) NOT NULL,
    initial_call_timestamp timestamp with time zone DEFAULT now(),
    group_call text,
    call_price numeric(30,10) NOT NULL,
    token_age integer,
    dexscreener_url text,
    best_pair_address character varying(66),
    group_id bigint,
    update_interval integer DEFAULT 300,
    last_updated_at timestamp with time zone,
    failed_updates_count integer DEFAULT 0,
    is_active boolean DEFAULT true
);
```
- Primary Key: `token_id`
- Foreign Keys: `group_id` references `telegram_groups(group_id)`
- Unique Constraints: `contract_address`
- Indexes: 
  - `idx_tokens_blockchain` on `blockchain`
  - `idx_tokens_contract` on `contract_address`
  - `idx_tokens_group_id` on `group_id`
  - `idx_tokens_ticker` on `ticker`
  - `idx_tokens_update_time` on `(last_updated_at, update_interval, is_active)`

**`token_calls`**:
```sql
CREATE TABLE public.token_calls (
    call_id bigint DEFAULT nextval('public.token_calls_call_id_seq'::regclass) NOT NULL,
    token_id bigint NOT NULL,
    call_timestamp timestamp with time zone NOT NULL,
    call_price numeric(30,10) NOT NULL,
    message_id bigint,
    note text,
    created_at timestamp with time zone DEFAULT now()
);
```
- Primary Key: `call_id`
- Foreign Keys:
  - `token_id` references `tokens(token_id)`
  - `message_id` references `telegram_messages(message_id)`
- Indexes:
  - `idx_token_calls_time` on `call_timestamp DESC`
  - `idx_token_calls_token` on `token_id`

**`price_metrics`** (TimescaleDB hypertable):
```sql
CREATE TABLE public.price_metrics (
    token_id bigint NOT NULL,
    pair_address character varying(66),
    "timestamp" timestamp with time zone NOT NULL,
    price_native numeric,
    price_usd numeric,
    txns_buys integer,
    txns_sells integer,
    volume numeric,
    liquidity_base numeric,
    liquidity_quote numeric,
    liquidity_usd numeric,
    fdv numeric,
    market_cap numeric,
    mongo_id text
);
```
- Primary Key: `(token_id, timestamp)`
- Foreign Keys: `token_id` references `tokens(token_id)`
- Indexes:
  - `idx_price_metrics_timestamp` on `timestamp DESC`
  - `idx_price_metrics_token_id` on `token_id`
  - `price_metrics_timestamp_idx` on `timestamp DESC`

#### **MongoDB Collections:**
- `dexscreener_data`: Stores raw API responses before processing
  - Fields: `blockchain`, `tokens`, `raw_data`, `processed`, `created_at`

### **Rules to follow before making changes:**
1. **Analyze my code first.** Identify inefficiencies, redundant logic, potential security risks, or any code smells.
2. **Optimize performance.** This project processes large amounts of real-time data. Ensure minimal memory usage, avoid blocking operations, and suggest better async handling.
3. **Refactor with clarity.** Simplify complex functions, ensure modularity, and improve naming conventions while maintaining functionality.
4. **Enhance database efficiency.** Ensure proper indexing in PostgreSQL, optimize queries, and validate the MongoDB schema for fast lookups.
5. **Ensure safe concurrent execution.** Validate that Celery tasks, API calls, and async Telegram operations are properly managed to avoid race conditions or memory leaks.
6. **Preserve existing functionality.** All refactoring must maintain intended behavior, especially in database interactions and API calls.
7. **Explain all changes.** Provide a concise summary of why each change is necessary and how it improves the code.
8. **Do not take anything for granted.** If you aren't sure about something, please ask before taking any assumptions.
9. **Always use SQL query files when possible.** Instead of crafting SQL queries in Python code, create and use .sql files from the sql/ directory for database operations.
10. **Verify database changes with dbclient.py.** After modifying code that affects the database, use the custom dbclient.py utility to verify that changes work as expected.

### **Project-specific optimizations:**
- **Connection management:** Ensure proper handling of database connections with proper closing to avoid leaks
- **Error handling:** Implement robust try-except blocks with specific error types and proper logging
- **API resilience:** Add retry logic and rate limiting for external API calls
- **Celery task optimization:** Ensure tasks are idempotent and handle worker failures gracefully
- **MongoDB connection handling:** Properly close connections after use to avoid exhausting the connection pool
- **Query optimization:** Use prepared statements and batch operations where possible
- **Index utilization:** Ensure queries are designed to use existing indexes effectively
- **SQL file usage:** For all database operations, create and use SQL files in the sql/ directory rather than embedding SQL in code

### **Key data relationships to maintain:**
1. Messages → Groups: Each message belongs to a specific Telegram group
2. Messages → Tokens: Messages may reference tokens (optional relationship)
3. Token Calls → Tokens: Each call is for a specific token
4. Token Calls → Messages: Calls are derived from specific messages
5. Price Metrics → Tokens: Price data is stored for each token over time
6. Tokens → Groups: Tokens can be associated with the group they were first mentioned in

### **Common error patterns to avoid:**
1. **Connection leaks:** Not closing MongoDB connections after use
2. **Uncaught exceptions:** Especially in Celery tasks which can cause silent failures
3. **Inadequate timeouts:** API calls without proper timeouts causing hung tasks
4. **Redundant API calls:** Making multiple calls for data that could be batched
5. **Transaction management issues:** Not properly committing PostgreSQL transactions
6. **Incorrect column references:** Always use the exact column names as defined in the schema
7. **TimescaleDB considerations:** Remember that price_metrics is a hypertable with special optimization requirements
8. **Embedded SQL:** Avoid writing SQL directly in Python code; use SQL files instead

### **Database access and verification:**
1. **Use dbclient.py for verification:** Always use the custom database client utility to verify changes work as expected
2. **PostgreSQL verification:** Run `python tools/db/dbclient.py pg "YOUR_QUERY"` to verify changes to PostgreSQL data
3. **MongoDB verification:** Run `python tools/db/dbclient.py mongo COLLECTION_NAME '{"query": "here"}'` to check MongoDB data
4. **Before/after verification:** Run queries before and after code changes to confirm the expected database state changes
5. **Error investigation:** Use dbclient.py to query the database when debugging issues or investigating errors
6. **Verify indexes:** Check that queries are using indexes with `EXPLAIN ANALYZE` via dbclient.py when optimizing database operations
7. **Data integrity:** Verify referential integrity across tables after operations that modify multiple related tables

### **Database query approach:**
When querying or modifying the database:
1. **Create SQL files:** Place all database queries in .sql files within the sql/ directory
2. **Use descriptive names:** Name files clearly based on their function (e.g., get_active_tokens.sql, update_token_status.sql)
3. **Parameterize queries:** Use placeholders (%s) for parameters to prevent SQL injection
4. **Load queries at runtime:** Read the SQL files and execute them with parameters instead of embedding SQL in code
5. **Document SQL files:** Include comments in SQL files explaining their purpose and parameters
6. **Check results on DB:** When responding to user questions, always run appropriate SQL queries to check database state
7. **Common verification commands:** 
   ```bash
   # Check token counts
   python tools/db/dbclient.py pg "SELECT COUNT(*) FROM tokens"
   
   # Verify token status
   python tools/db/dbclient.py pg "SELECT token_id, name, is_active FROM tokens WHERE token_id = 123"
   
   # Check MongoDB processing status
   python tools/db/dbclient.py mongo dexscreener_data '{"processed": false}' --limit 5
   ```

### **Useful commands for debugging:**
```bash
# Extract database schema
pg_dump -U bot -d crypto_db --schema-only > crypto_db_schema.sql

# Check MongoDB connection
python -c "from scripts.utils.db_mongo import connect_mongodb; connect_mongodb().server_info()"

# Run Celery worker with debug logging
celery -A scripts.price_tracker.celery_app worker --loglevel=debug

# Start beat scheduler for periodic tasks
celery -A scripts.price_tracker.celery_app beat

# Query most recent price metrics
python tools/db/dbclient.py pg "SELECT * FROM price_metrics ORDER BY timestamp DESC LIMIT 10"

# Check token update status
python tools/db/dbclient.py pg "SELECT token_id, name, last_updated_at, failed_updates_count FROM tokens ORDER BY last_updated_at DESC LIMIT 10"
```

Always ensure changes are tested, maintain compatibility across all project modules, and follow best Python practices.

**Critical performance areas:**
- Telegram message processing pipeline
- DexScreener API interaction
- MongoDB data storage and retrieval
- TimescaleDB hypertable querying and insertion
- Celery task scheduling and execution
