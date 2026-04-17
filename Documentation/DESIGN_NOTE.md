# Design Note

## 1. Architecture
The solution is organized as a batch enrichment pipeline with a small number of clear layers.

The orchestration layer lives in [enrich_daycare_data.py](C:\Users\deepa\OneDrive\Documents\DayCare-Scrapper\enrich_daycare_data.py). It reads the input CSV, prepares cleaned input, selects rows, coordinates enrichment, saves checkpoints, and writes the final output CSV. This keeps the workflow control in one place.

The source-access layer is split into registries and adapters. State-specific APIs and portal scrapers are kept separate from fallback web-search logic. That matters because the sources are heterogeneous: some states have structured APIs, some have searchable web portals, and some require browser-driven extraction.

The fallback layer uses Google and Winnie. The pipeline tries more authoritative structured sources first, then uses broader search-based methods only when necessary. This helps reduce scraping cost and keeps source quality as high as possible.

The persistence layer stores operational state locally:
- checkpoints for successful enrichment
- Google-specific checkpointing
- persisted bad Google proxy hosts
- a persisted Google miss registry
- logs

This architecture is simple enough for a local evaluation project, but still modular enough to extend.

## 2. Scale and cost
At 1,000 rows, the current design is workable on a single machine. At 1 million rows, the main bottlenecks would be browser automation, retries, blocking, and external-site latency.

The biggest cost driver is Google/browser-based fallback. Browser automation is expensive in both time and money compared with structured APIs. At large scale, the direct money costs would likely come from:
- compute for long-running worker machines or containers
- proxy infrastructure, especially if residential or rotating proxies are needed
- storage for checkpoints, logs, and intermediate artifacts
- monitoring and alerting infrastructure
- engineering and operations time spent maintaining source integrations

In practice, the most expensive path is not just CPU time. It is the combination of browser sessions, anti-bot handling, retries, and proxy consumption. If a large share of records falls through to Google fallback, the cost curve rises quickly because each unresolved record can consume multiple page loads, retries, and backoff windows before returning useful data or a miss.

Structured API calls are much cheaper per record. They are faster, easier to parallelize, and usually require less retry logic. Because of that, the primary cost strategy at 1 million rows would be to maximize structured-source coverage and minimize fallback usage.

To control cost at larger scale, I would:
- batch work by state and source
- prioritize APIs over portals and portals over browser-heavy fallback
- keep checkpointing aggressive so reruns avoid repeat work
- treat Google/Winnie as exception paths rather than standard paths
- distribute work across workers by state or source family
- monitor miss rates so low-yield paths can be reduced or disabled

I would also add explicit cost controls, for example:
- per-source budgets or daily request caps
- stopping rules when a source’s success rate drops below a threshold
- separate queues for cheap structured work versus expensive browser work
- dynamic throttling so proxy-heavy traffic is limited during periods of poor yield
- periodic ROI reviews on fallback sources to decide whether a source is still worth running

If this were deployed in the cloud, I would expect the main budget categories to be:
- worker compute
- proxy/vendor fees
- observability tooling
- storage and job orchestration

The simplest way to keep the bill under control would be to make the expensive sources rare, measurable, and easy to turn down when yield is poor.

At that size, a queue-based worker architecture would be more appropriate than a single-machine batch script.

## 3. Production readiness
To move this to production, I would add stronger observability, scheduling, quality controls, and security hygiene.

For observability:
- success and failure rates by source
- field fill rates for address, ZIP, phone, and URL
- retry counts, anti-bot counts, and browser failure counts
- latency metrics by source and state

For reliability:
- scheduled jobs with restartable workers
- idempotent task execution
- clearer separation between transient failure, hard miss, and source outage
- quarantine queues for problematic records

For data quality:
- validation for normalized phone and ZIP formats
- confidence monitoring over time
- drift detection when a source suddenly stops producing expected fields
- sample-based review workflows

For security:
- move sensitive runtime settings and credentials into environment-based secrets management
- minimize sensitive content in logs
- harden any proxy credential handling

## 4. Maintenance
The hardest part of maintaining a live enrichment system is external change. Websites change layout, labels, pagination, anti-bot rules, and sometimes the meaning of fields.

The main risks are:
- state portal HTML changes
- Google result layout drift
- increased blocking or CAPTCHA behavior
- silent data-quality degradation
- growing complexity as more source-specific rules are added

I would mitigate those risks by:
- keeping source-specific parsing isolated
- adding regression tests using saved fixtures for important layouts
- monitoring field-fill drift and failure spikes
- making fallback behavior configurable
- using persisted miss and bad-proxy registries to avoid wasting repeated effort

In production, maintenance would be partly operational, not just code-based. Regular review of logs, source-health metrics, and drift signals would be necessary to keep the system stable.
