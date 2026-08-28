# SMS Warehouse Builder (v7) — Technical Report

## 1. Purpose

The script turns a raw CSV of SMS messages — sent by Indian brokers, depositories (NSDL/CDSL), exchanges (NSE/BSE/MCX), and fintech apps (Groww, Zerodha, Angel One, MOFSL, Upstox, Dhan) — into a single, structured Excel workbook. Every SMS is:

1. **Classified** into one specific category via regex rules (e.g. "NSDL pay-in debit", "Angel One margin shortfall").
2. **Parsed** for structured fields (dates, amounts, quantities, security names, account masks, etc.) using named regex groups.
3. **Routed** into one or more of 12 normalised output tables based on what type of event it represents.

The output is one `.xlsx` file with all tables as separate sheets, plus QA helper sheets.

## 2. Pipeline Flow (`run()` — Section 11)

```
CSV → detect columns → classify every SMS → build enriched master table
    → route rows into 12 normalised tables → backfill Broker_Associations
    → write everything to one Excel workbook
```

Step by step:
1. Reads the CSV header first to auto-detect key columns (client ID, message body, SMS ID, timestamp) using flexible name-matching (`pick_column` + `BASE_HINTS`), so it tolerates different header spellings.
2. Reads the full CSV, cleans whitespace in every message (`clean_text`).
3. Classifies every message (`classify_all`) — optionally in parallel via `ProcessPoolExecutor` if `USE_MULTIPROCESSING=True` and the file is large.
4. Builds an "Enriched_Master" DataFrame: one row per SMS with its winning classifier, parsed fields, and broker name.
5. Routes every row into the 12 output tables (`build_tables`).
6. Cross-checks that every (client, broker) pair seen anywhere appears at least once in `Broker_Associations`, and drops empty/junk rows there.
7. Writes one Excel workbook with all sheets via `pd.ExcelWriter`.

## 3. Classification Engine (Sections 5, 6, 8)

- **`RULES`** is a dict keyed by sender "family" (NSDL, CDSL, NSE, BSE, ANGELONE, GROWW, MOFSL, ZERODHA, MCX, UPSTOX, DHAN, ACCOUNT, GENERIC). Each family holds a list of `(label, compiled_regex)` pairs. Regexes use named groups prefixed `rx_` (e.g. `rx_date`, `rx_qty`) to extract structured fields directly during matching.
- **`build_rule_index()`** flattens this nested dict into one flat list of `(family, label, canonical_key, pattern)` for fast iteration.
- **`classify(text, rule_index)`** runs every rule against the SMS body. The **first matching rule wins** (families/rules are checked in dict order) and its named groups become the parsed fields. All other rules that also matched are recorded separately as "hits" for QA (to catch ambiguous/overlapping messages).
- **`ALIASES`** (Section 6) maps every rule label to a canonicalised, punctuation-stripped key (via `canon()`). This canonical key is what the routing logic in `build_tables()` checks against (e.g. `"PAYINDEBIT" in c`), decoupling routing logic from the exact label spelling.
- Unmatched messages get the special classifier `"UNCATEGORISED"`.

## 4. Field Normalisation Helpers (Sections 1–4)

- `clean_text` — collapses whitespace/nbsp.
- `to_num` — extracts a float from strings like `"1,23,456.78"`.
- `parse_date` — tries a list of Indian date formats, then falls back to pandas' `dayfirst=True` parsing.
- `norm_fields` — strips the `rx_` prefix from every matched group and merges synonym field names (e.g. `ac_no` → `acno`) via an alias-pairs map, so downstream code can look up one canonical key regardless of which specific rule matched.
- `val(f, *keys)` — returns the first non-empty value among several possible field-name candidates.
- `clean_security_name` — strips boilerplate ("Ordinary Shares -", trailing ISINs, face-value text, depository suffixes) from a raw security name.
- `detect_segment` / `detect_mf_or_etf` — classify a security as EQ vs ETF (or MF vs ETF) using an `_ETF_KEYWORDS` keyword list.
- `holdings()` — parses a comma-separated "qty - name, qty - name" string (used in bulk CDSL debit/credit messages) into a list of `(qty, name)` tuples.
- **`BROKERS`** — a lookup table mapping raw broker-name keywords to canonical display names (e.g. "MOTILALOSWAL" → "Motilal Oswal"). `normalize_broker_name()` uses this, falling back to title-casing unknown names.
- `broker_from_sms()` — determines the broker for an SMS in three passes: (1) trust the regex-parsed field if present, (2) keyword-scan the whole SMS body against `BROKERS`, (3) fall back to structural text patterns like "Regards, <Broker>".
- `detect_trading_segment` — infers Cash Equity / F&O / Commodity / Currency from body text.
- `exchange_from` — best-effort NSE/BSE/MCX detection from classifier label + body.

## 5. Output Tables (Section 7)

`TABLE_COLUMNS` defines the schema for 12 sheets:

| Table | Captures |
|---|---|
| Broker_Associations | Client↔broker registration, UCC, activation/closure dates |
| Transactions | Share movements, trades, payouts, corporate actions |
| Pledge_Activity | Pledge / unpledge / invocation events |
| EOD_Balances | End-of-day fund & securities balances |
| Mutual_Funds | SIP / lump-sum / MF conversions |
| IPO_Lifecycle | IPO application → allotment tracking |
| KYC_Changes | Mobile/bank/address/nominee/signature changes |
| Account_Events | Account open/close/TPIN/e-voting/misc lifecycle events |
| Margin_Risk_Alerts | Margin calls, MTM loss, shortfalls, physical-delivery/expiry warnings |
| Statements_Docs | CAS, contract notes, PnL report delivery |
| Portfolio_Valuations | Point-in-time portfolio value snapshots |
| Advisory_Promo | Promos, regulatory advisories, uncategorised catch-all |

Every row also carries common columns: `client_id`, `source_classifier`, `source_sms_id`, `event_timestamp`, `full_sms`.

## 6. Routing Logic (`build_tables()` — Section 9)

For each classified SMS, the function checks the canonicalised classifier key `c` against lists of trigger substrings and appends a row to the matching table(s). **One SMS can land in multiple tables** — e.g. a pledge-invocation SMS produces rows in both `Transactions` and `Pledge_Activity`.

Key routing blocks, in order:
1. **Broker_Associations** — UCC/account open/close/activate/POA/DDPI triggers.
2. **Transactions** — share debit/credit/blocked, MF units movement, pledge-invocation debits, trade executions, buybacks, gifts, corporate actions (bonus/split/merger/scheme/extinguishment), redemption certificates, traded-value summaries, payouts/settlements.
3. **Pledge_Activity** — pledge/unpledge/invocation/margin-pledge/auto-pledge events.
4. **EOD_Balances** — fund/securities balance reports.
5. **Mutual_Funds** — SIP/lump-sum/conversion events, with SIP-vs-lumpsum inferred via `is_sip_from_text`.
6. **IPO_Lifecycle** — application submitted, allotted/not-allotted, bidding open.
7. **KYC_Changes** — nominee/mobile/signature/address/bank/contact/income field changes.
8. **Account_Events** — account lifecycle (open/close/suspend/activate), DDPI/POA/insta-demat registration, TPIN generation, e-voting, welcome/ISIN/TWCP notices.
9. **Margin_Risk_Alerts** — margin utilisation, MTM loss, shortfalls, physical delivery/expiry warnings, trade-confirmation calls, MCX price alerts.
10. **Statements_Docs** — CAS statements, derivative PnL reports, contract notes.
11. **Portfolio_Valuations** — CDSL portfolio value snapshots.
12. **Advisory_Promo** — final catch-all: anything not placed in another table (advisory/tips warnings, promotional messages, or truly `UNCATEGORISED` messages) lands here, sub-classified into security/regulatory/promo/alert/uncategorised.

A `_placed_before`/`_placed_elsewhere` row-count check prevents a message from being double-logged in the catch-all bucket once it's already been routed somewhere meaningful.

## 7. Post-processing: `ensure_all_clients_in_broker_table()` (Section 10)

After all tables are built, this function scans every table for `(client_id, broker_name)` pairs and makes sure each one is represented at least once in `Broker_Associations`, inserting "inferred" rows where missing. It also drops any `Broker_Associations` rows where both `broker_name` and `full_sms` are empty (no useful information).

## 8. Output Workbook (Section 11)

The final `.xlsx` contains:
- `Enriched_Master` — every SMS with its full classification + parsed fields.
- The 12 normalised tables above.
- `Row_Summary` — row count per table (quick QA check).
- `Multi_Rule_Hits` — SMS that matched more than one regex rule (helps spot ambiguous/overlapping rules).
- `Uncategorised` — SMS that matched no rule at all (candidates for writing new rules).

## 9. Performance Notes

- `USE_MULTIPROCESSING` (default `False`) enables `ProcessPoolExecutor`-based parallel classification for files over ~10,000 rows (`CLASSIFY_CHUNK_SIZE = 5,000` per chunk), useful for very large inputs (>500k rows per the header comment).
- Since regex matching is the dominant cost and rules are checked sequentially per family, the ordering of `RULES` matters: more specific/common patterns early reduce wasted matching, though correctness (first-match-wins) is unaffected by ordering as long as rules don't unintentionally overlap.

## 10. How to Run

Edit `INPUT_CSV` and `OUTPUT_DIR` at the bottom of the file (Section 12), then run:
```
python sms_warehouse_v7.py
```
This produces `sms_warehouse.xlsx` in the output folder.

## 11. Extensibility ("change when required" markers)

The code is heavily annotated with "change when required" comments at every place a maintainer would likely need to edit it: adding brokers to `BROKERS`, adding ETF keywords, adding new regex rules to `RULES` (remembering to also add the alias to `ALIASES`), adjusting `TABLE_COLUMNS`, or adding new alternate CSV header spellings to `BASE_HINTS`.
