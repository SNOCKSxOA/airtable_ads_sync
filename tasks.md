# airtable_ads_sync — tasks

## Done

- **Leslunes sync** — `main.py` maps canonical label `Leslunes` to Airtable base `appUqN503Qs24duGz`, table `tblkAk6Q9ZgG9LyTz`, and BigQuery `company = 'leslunes'` in `marts_finance_euw3.ad_create_roas`. HTTP Cloud Function runs Snocks, OA, and Leslunes in one invocation.

## Verified (2026-04-16)

- **BQ** `snocks-analytics.marts_finance_euw3.ad_create_roas` WHERE `company = 'leslunes'`: **7** rows; `extracted_CR_number` present on all 7.
- **Code path** `fetch_bq_rows('Leslunes')` returns **7** rows; `_airtable_url('Leslunes')` → `https://api.airtable.com/v0/appUqN503Qs24duGz/tblkAk6Q9ZgG9LyTz`.
- **Airtable** `fetch_airtable_index('Leslunes', …)` against that base/table succeeds (**19** records with “Creative ID” populated). PAT via Secret Manager `airtable_full_acces_api_key`.

## Notes

- Local: `python main.py` runs OA, Snocks, then Leslunes (requires ADC / secrets as before).
- Aliases accepted for `run_sync` / normalization: `les lunes`, `les_lunes`, `LESLUNES`, etc. (normalized to `leslunes` before matching).
