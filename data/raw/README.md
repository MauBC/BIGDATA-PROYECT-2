---
license: cc-by-nc-4.0
size_categories:
  - 100K<n<1M
language:
  - en
task_categories:
  - text-generation
  - text-classification
tags:
  - reddit
  - social-media
  - comments
  - conversation
  - conversational-tree
pretty_name: Reddit Comment Trees Sample — Initial snapshot
configs:
  - config_name: posts
    data_files:
      - split: train
        path: data/posts/**/*.parquet
  - config_name: comments
    data_files:
      - split: train
        path: data/comments/**/*.parquet
---

# Reddit Comment Trees Sample — Initial snapshot

> **Initial sample: 43,913 posts and 59,874 comments across three communities.**
> This is a selected, structurally checked snapshot, with incomplete subreddit coverage. It is not a complete three-month archive.

## Overview

Posts and associated comments from **r/LocalLLaMA, r/wallstreetbets, and r/SkincareAddiction**.
The requested post window is **June 1–August 31, 2026 in Asia/Shanghai**, with UTC bounds **2026-05-31 16:00:00 inclusive to 2026-08-31 16:00:00 exclusive**. This window is not calendar Q2 or Q3.
Only candidates with resolved stored relationships, no outstanding pagination or review flags, and counts matching the collector's expected count are included. These checks do not prove upstream recall.
`link_id`, `parent_id`, and recomputed `depth` preserve the relationships in the selected records.

## Why this dataset exists

Public Reddit archives include data after the 2023 API changes; freshness alone is not a unique feature.
This sample focuses on documented time coverage, explicit post/comment relationships, typed columns, and reproducible collection snapshots.
The three communities provide examples of AI discussion, market discussion, and consumer product feedback.
The release will report observed gaps instead of treating unavailable records as successfully collected data.

## Coverage

| Subreddit | Posts | Comments | Observed post timestamps (UTC, inclusive endpoints) | Maximum depth |
|---|---:|---:|---|---:|
| LocalLLaMA | 3,778 | 21,850 | 2026-05-31T16:13:21+00:00 to 2026-08-31T15:31:52+00:00 | 15 |
| wallstreetbets | 10,310 | 12,500 | 2026-05-31T16:17:43+00:00 to 2026-08-31T15:53:36+00:00 | 10 |
| SkincareAddiction | 29,825 | 25,524 | 2026-05-31T16:04:12+00:00 to 2026-08-31T15:58:30+00:00 | 9 |

Observed endpoints do not establish gap-free coverage. Historical post discovery remains incomplete.
The sample includes zero-comment posts and disproportionately small comment trees; it is **not a representative or balanced sample** of these communities.
The date window selects posts. Associated comments retain their collection-time snapshot, including comments newer than the post window.
Per-community comment dates, depth distributions, deletion/removal ratios, null counts and file checksums are measured in [stats/summary.json](stats/summary.json).

## Schema

Both tables use **Parquet with Zstandard compression**. Text is not truncated; timestamps are Unix seconds in UTC.
Media files are not included. Preserve source text, except any documented anonymization.
No additional anonymization is applied. Missing or unsupported scalar fields remain null, including unknown authors; null is not converted to `[deleted]`. Community names are normalized against each linked post. All non-key scalar fields may be null.

### Posts

| Field | Type | Description |
|---|---|---|
| id | string | Post ID without the `t3_` prefix |
| subreddit | string | One of the three listed communities |
| author | string | Source author; deleted accounts use `[deleted]` |
| created_utc | int64 | Post creation time, Unix seconds UTC |
| title | string | Original post title |
| selftext | string | Original post text; empty string for a link post |
| url | string | External URL or post permalink |
| score | int32 | Score snapshot at collection time |
| upvote_ratio | float32 | Source upvote ratio |
| num_comments | int32 | Reddit's displayed count; may differ from collected rows |
| flair | string | Nullable post flair |
| is_nsfw | bool | Source NSFW flag |
| permalink | string | Reddit permalink |
| crawled_at | int64 | Collection time, Unix seconds UTC |
| batch_id | string | Collection batch identifier |

### Comments

| Field | Type | Description |
|---|---|---|
| id | string | Comment ID without the `t1_` prefix |
| subreddit | string | Same community as the linked post |
| link_id | string | Linked post ID without the `t3_` prefix |
| parent_id | string | Unprefixed parent ID; top-level comments use `link_id` |
| depth | int16 | Top-level comments have depth 0 |
| author | string | Source author; deleted accounts use `[deleted]` |
| created_utc | int64 | Comment creation time, Unix seconds UTC |
| body | string | Original text; preserve `[deleted]` and `[removed]` markers |
| score | int32 | Score snapshot at collection time |
| is_submitter | bool | Whether the author is the post's submitter |
| permalink | string | Reddit permalink |
| crawled_at | int64 | Collection time, Unix seconds UTC |
| batch_id | string | Collection batch identifier |

Resolve a depth-0 parent against **posts**; resolve a deeper parent against **comments**, within the same subreddit and `link_id`.
Post and comment IDs belong to different namespaces even when their prefixes are removed.
The two HF configurations are `posts` and `comments`, each with a `train` split used as a storage partition, not a predefined ML training benchmark.
Subreddit folders group files; they do not automatically become separate HF configurations.

## Collection method and known gaps

Collected through an existing Reddit business API using multiple third-party providers. Initial and continuation results were merged by post/comment ID. The exported snapshot was prepared at **2026-09-12T14:48:06+00:00**.

All selected records pass post-scope, unique-ID, parent-resolution, cycle, derived-depth and stored-comment-count checks. Comment text was compared with the source database without truncation. At least 24 randomly selected comments were manually inspected across the three communities. Local Parquet loading was checked with datasets, pandas and DuckDB.

Full tree completeness and complete subreddit recall are **not asserted**: unavailable posts, source-side omissions, historical search gaps and all unresolved review records are outside this sample. Placeholder comment IDs such as `deleted` are excluded by excluding affected threads; valid IDs with `[deleted]` or `[removed]` text are retained.

Source usernames and public text are retained when provided. Moderator and bot replies, removed-content notices, informal language and potentially offensive text may be present; this is not a filtered human-only conversation dataset. Media, credentials, private messages and raw API responses are not included.

Scores, displayed post comment counts and comment rows come from different collection snapshots. The displayed `num_comments` field is not a lifetime total or a completeness guarantee.

## Load the data

Choose one of the two real Parquet configurations. The files contain source records, not placeholder rows.
Choose a config explicitly because posts and comments have different schemas.
See [examples/load.py](examples/load.py) for separate `datasets`, pandas, and DuckDB examples.

```python
from datasets import load_dataset
comments = load_dataset("mashu-data/reddit-comments-sample", "comments", split="train", streaming=True)
print(next(iter(comments)))
```

```python
import duckdb
from huggingface_hub import hf_hub_download

path = hf_hub_download(
    repo_id="mashu-data/reddit-comments-sample",
    repo_type="dataset",
    filename="data/comments/subreddit=LocalLLaMA/part-0.parquet",
)
con = duckdb.connect()
con.execute("""
    SELECT id, link_id, parent_id, depth
    FROM read_parquet(?, hive_partitioning=false) LIMIT 20
""", [path]).df()
```

## Full dataset and custom delivery

**Need more communities, a longer time range, or specific fields? [Contact Mashu Data](https://mashudata.com/contact.html).**
Ask about verified batch availability and custom subreddit, date-range or field selection before ordering. This sample does not establish availability of 20 communities, a complete 12-month archive or scheduled weekly updates.
Website: [mashudata.com](https://mashudata.com).

## License and use

The sample is designated **CC BY-NC 4.0** for non-commercial research use, to the extent Mashu Data holds rights in the released material.
Reddit users retain rights in their content; the repository license does not replace those rights or grant rights held by third parties.
For commercial use or delivery terms, [contact Mashu Data](https://mashudata.com/contact.html).
Cite the dataset repository and the exact revision used in your work; this dataset is not affiliated with or endorsed by Reddit.
