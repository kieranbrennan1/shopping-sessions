# Shopping Sessions (Builders Buildathon · Databricks track)

One Rust binary that turns the click-stream CSVs in `/data` into `/output/user_daily/` and
`/output/campaign_daily/` Parquet tables, following `rules.md` exactly.

How it works:
1. Memory-maps every CSV and splits the files into ~4 MB line-aligned chunks, parsed on all cores.
2. Links: union-find over every linked ID; each person is named by the byte-wise smallest ID.
3. Events are bucketed by a 64-bit hash of `event_id`, deduplicated (smallest `ingest_ts` wins) and
   validated; clicks and surviving events are bucketed by a hash of the person's name.
4. Each person bucket is sorted, cut into 30-minute sessions, credited to the latest click within
   7 days (ties: smallest `click_id`), with assists from a sliding 7-day window of campaigns.
5. Both tables are written as uncompressed Parquet, one file per worker thread.

Every hash match is confirmed on the real bytes; if two different strings ever share a hash, that
bucket is redone with exact string keys (`SS_EXACT=1` forces exact mode everywhere, for testing).

Checked against the 36 starter fixtures, the 1M-event dev answer, and a 20M-event set.
