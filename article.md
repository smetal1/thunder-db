# Why I Built ThunderDB: From GPU Cloud Nightmares to Database Revolution

*How building Podstack's real-time billing system broke everything we knew about databases—and forced us to build something new*

---

## The Invoice That Broke Everything

It started with an angry email from a customer in Bangalore.

"Your invoice says I owe ₹47,000 but I only used the GPU for 3 hours. Your platform is a scam."

I stared at the email, then at our billing dashboard, then at our PostgreSQL database. The numbers didn't match. They never matched anymore.

At Podstack, we'd built something ambitious: a GPU cloud platform with pay-per-second billing. Users could spin up an A100 for exactly 47 seconds of inference and pay only for those 47 seconds. No hourly minimums. No rounding. Precision billing that made GPU computing accessible to indie developers and startups who couldn't afford reserved instances.

It was a beautiful vision. And our database architecture was killing it.

---

## The Monster We Created

Podstack wasn't a simple CRUD app. It was a beast.

Every second, we ingested metrics from thousands of running containers—CPU ticks, memory bytes, GPU utilization, network packets. Each metric needed to be stored, aggregated, and converted into billing events. Users expected real-time dashboards showing their current spend, their burn rate, their projected monthly cost.

Our architecture looked like every "modern data stack" diagram you've seen at conferences:

```
PostgreSQL (transactions)
    → Kafka (streaming)
        → Spark (processing)
            → ClickHouse (analytics)
                → Redis (caching)
                    → Elasticsearch (search)
```

Six systems. Six failure modes. Six things to monitor at 3 AM.

The PostgreSQL handled user accounts, projects, and billing records. Kafka streamed the metrics. Spark aggregated them into billable units. ClickHouse stored the time-series for dashboards. Redis cached the hot data. Elasticsearch powered the audit log search.

Every arrow in that diagram was a lie waiting to happen.

---

## Death by a Thousand Pipelines

The customer's ₹47,000 invoice bug? It took us three days to find.

Here's what happened: A Kafka consumer had lagged during a traffic spike. The metrics arrived late. Spark processed them with yesterday's pricing (we'd had a promotional discount that expired). ClickHouse showed the correct usage, but PostgreSQL had the wrong billing amount. Redis was caching stale totals. The invoice PDF was generated from PostgreSQL. The dashboard showed ClickHouse numbers.

Two sources of truth. Two different truths.

We fixed that bug. Then we fixed the next one. And the next. Every week brought a new variation:

- CDC lag causing billing delays
- Schema drift between PostgreSQL and ClickHouse
- Spark job failures during deploys
- Redis cache invalidation races
- Elasticsearch index corruption

Our ops team was playing whack-a-mole with data consistency. Our engineers spent more time debugging pipelines than building features. Our customers lost trust every time the numbers didn't add up.

**We weren't running a GPU cloud. We were running a distributed systems disaster recovery operation.**

---

## The 3 AM Epiphany

It was 3 AM—it's always 3 AM—when I finally asked the question I'd been avoiding:

*Why do we have six databases?*

The conventional wisdom said we needed them. OLTP databases are for transactions. OLAP databases are for analytics. Time-series databases are for metrics. Search engines are for... search. Each tool optimized for its use case.

But sitting there, SSH'd into production, watching data trickle between systems that should have been one system, I realized something:

**The complexity wasn't solving our problem. It was our problem.**

Every integration point was a failure point. Every data copy was a consistency risk. Every additional system was another thing to scale, secure, and pay for.

What if we didn't need six databases? What if we needed one?

---

## The Napkin Architecture

I grabbed a notebook and started sketching.

Podstack needed a database that could:

1. **Handle transactions** — User accounts, billing records, ACID guarantees
2. **Ingest time-series** — Millions of metrics per minute, append-only, fast writes
3. **Power analytics** — Real-time aggregations, GROUP BY queries, dashboards
4. **Support vectors** — We were adding AI features, semantic search for models
5. **Scale horizontally** — Our growth was 10x year-over-year
6. **Stay consistent** — One truth, always, everywhere

No existing database did all of this. PostgreSQL was great for transactions but choked on analytical queries over billions of metrics. ClickHouse was blazing fast for analytics but couldn't handle our transactional workloads. TimescaleDB was a compromise that satisfied neither need fully.

The more I researched, the more I realized: the OLTP/OLAP split wasn't a law of physics. It was a historical artifact from an era of spinning disks and single-core CPUs.

Modern hardware had changed everything. NVMe drives with microsecond latency. Servers with terabytes of RAM. CPUs with 128 cores and SIMD instructions. Memory-safe languages that could match C++ performance.

**The technology existed. Someone just needed to put it together.**

That someone would have to be us.

---

## The First Commit

I wrote the first line of ThunderDB on a Sunday afternoon, three months after that billing incident.

I chose Rust. Not because it was trendy, but because it was honest. Databases are unforgiving software. A memory bug in a database doesn't just crash your process—it corrupts your customers' data. Rust's compiler would catch the bugs that would have haunted us for years in C++.

The core insight was simple: **store data twice, query it once.**

Every row would exist in two forms:
- A **row store** for fast point lookups and transactional updates
- A **column store** for analytical scans and aggregations

When Podstack wrote a billing event, ThunderDB would store it in the row format immediately (for transaction speed) and convert to columnar format asynchronously (for analytics speed). When the dashboard queried "total spend by GPU type this month," ThunderDB would scan the column store with vectorized SIMD operations.

"But that's expensive!" you might say. "You're storing everything twice!"

Yes. And it's still cheaper than running six databases with armies of engineers to keep them in sync.

---

## The Features We Actually Needed

Building ThunderDB for Podstack meant building exactly what we needed—no more, no less.

**Wire Protocol Compatibility**: Our codebase had thousands of PostgreSQL queries. Rewriting them was not an option. ThunderDB speaks PostgreSQL wire protocol. We changed the connection string. Everything worked.

**Distributed Transactions**: Podstack runs across multiple regions. When a user in Mumbai launches a GPU in Frankfurt, that's a distributed transaction. ThunderDB uses Raft consensus and two-phase commit. ACID guarantees, globally.

**Vector Search**: Our AI Studio needed semantic model search. Instead of adding yet another database (Pinecone, Qdrant, etc.), we built HNSW indexes into ThunderDB. One query can now filter by project, sort by similarity, and paginate—all in SQL.

**Change Data Capture**: Some systems still needed event streams. Rather than bolting on Debezium, we built CDC into the storage engine. Subscribers get exactly-once delivery of every change.

**Real-time Aggregations**: The billing dashboard needs sub-second updates. ThunderDB maintains materialized rollups that update incrementally. No more waiting for Spark jobs.

Each feature eliminated an integration. Each integration eliminated was a failure mode removed, a consistency bug prevented, an on-call page avoided.

---

## The Migration

Six months after the first commit, we migrated Podstack to ThunderDB.

I won't pretend it was easy. We ran both systems in parallel for weeks, comparing query results, validating billing calculations, stress-testing with production traffic patterns.

But when we finally flipped the switch, the silence was deafening.

No more Kafka lag alerts. No more Spark job failures. No more cache invalidation bugs. No more "the dashboard shows different numbers than the invoice" support tickets.

**One database. One truth. One codebase to maintain.**

Our infrastructure cost dropped 40%. Our ops team went from firefighting to actually improving the platform. Our engineers started shipping features again instead of debugging pipelines.

And the billing discrepancies? They stopped. Completely.

---

## What Podstack Taught Me

Building ThunderDB for Podstack taught me lessons I couldn't have learned any other way:

**1. Complexity is not sophistication.**
Our six-database architecture looked impressive on diagrams. It was also the source of almost every production incident. The best architecture is the simplest one that solves the problem.

**2. Integration is where systems die.**
Every arrow in an architecture diagram is a promise that will eventually be broken. Network partitions, schema drift, version mismatches, timing bugs—integration points are where complexity hides.

**3. Build for your actual workload.**
Generic databases optimize for generic workloads. ThunderDB is opinionated because Podstack's workload was specific. Time-series metrics. Real-time billing. Transactional consistency. AI features. We built exactly what we needed.

**4. Performance is not optional.**
When your billing depends on processing millions of events per second, "good enough" isn't good enough. We optimized ruthlessly—jemalloc for memory allocation, SIMD for aggregations, io_uring for disk I/O.

---

## Beyond Podstack

ThunderDB started as Podstack's internal database. But the problem we solved isn't unique to GPU clouds.

Every company with real-time analytics is running our old architecture. PostgreSQL plus Kafka plus Spark plus a data warehouse plus Redis plus Elasticsearch. Every company is fighting the same integration bugs, paying the same infrastructure costs, losing the same engineer-hours to pipeline maintenance.

That's why ThunderDB is open source under Apache-2.0.

The 3 AM pages shouldn't be about your database. They should be about your actual product. Your engineers should build features, not maintain ETL pipelines. Your customers should trust that the numbers they see are the numbers that matter.

---

## The Road Ahead

ThunderDB is version 0.1.0. Podstack runs on it in production, but there's so much more to build.

I want ThunderDB to be the last database companies like ours need to think about. Not through feature bloat, but through thoughtful unification. Transactions and analytics. Vectors and relations. Time-series and documents. Real-time and historical.

The future of data infrastructure isn't more specialized databases connected by more pipelines. It's fewer, smarter systems that refuse to make you choose.

**That's why I built ThunderDB. Because the invoice that broke everything also broke the spell—the spell that said we had to accept this complexity as normal.**

We don't. And now, neither do you.

---

*ThunderDB is open source and available on GitHub. If you're drowning in database integrations, if your analytics are always stale, if your billing never matches your dashboard—give it a try. Connect with psql. Run the queries you've always wanted to run. See what happens when you stop fighting your infrastructure and start building your product.*

---

*Saurav is the founder of Podstack and creator of ThunderDB. He believes the best infrastructure is the kind you forget exists—because it just works.*
