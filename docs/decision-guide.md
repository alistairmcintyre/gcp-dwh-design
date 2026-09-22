# Decision guide

The choices in this repo written as "if this, then that", with what each option costs. Most
branches come from something built or measured here, and link to where the detail lives. The few
that don't are marked.

1. [Erasure requests](#1-erasure-requests)
2. [Schema evolution into BigQuery](#2-schema-evolution-into-bigquery)
3. [Access control](#3-access-control)
4. [dbt across many teams](#4-dbt-across-many-teams)

---

## 1. Erasure requests

Detail: [`gdpr-erasure.md`](gdpr-erasure.md). Code: [`privacy/`](../privacy).

### Where does the person's data live?

```
Kafka topic
├── keyed by the person
│   ├── can be compacted ................ tombstone it, with max.compaction.lag.ms set
│   └── needs time-based retention ...... encrypt the personal fields, destroy the key
└── keyed by something else (trade, order)
    └── a tombstone can't target a person, so encrypt at the producer and destroy the key

Warehouse table
├── the row exists only because the person does ...... DELETE it
├── the row has to be kept by law (trades, AML) ...... keep it, name the legal basis,
│                                                      remove the link to the person
└── an aggregate with no person column ............... leave it, and decide separately
                                                       whether totals get restated

Somewhere you can't edit (backups, partner extracts, old log segments)
└── crypto shredding is the only option, and it only works if the data was
    encrypted before it got there
```

### Then, for the process around it

| If | Do | Because | What it costs |
|---|---|---|---|
| the job might crash halfway | destroy the key **before** deleting rows | a crash then leaves unreadable data, not readable data | nothing |
| a topic's compaction is on defaults | set `max.compaction.lag.ms` | the active segment never compacts and the cleaner waits for a 50% dirty log, so a quiet topic can hold a tombstone for weeks | more cleaner work on the broker |
| a consumer can lag longer than `delete.retention.ms` (24h default) | drive erasure from a request table, not from the log | it will never see the tombstone | a table every system can read |
| the request has to take effect today, but deleting takes time | filter the person out of staging now, delete in the nightly sweep | the law asks you to stop *using* the data straight away (Article 18) | two paths to keep in step |
| facts are built from records you're obliged to keep | filter wherever per-person rows are **derived**, not only where the person is stored | otherwise the next full build brings them back | the filter lives in more than one model |
| something downstream has to match on the field | deterministic encryption, keyed per person | joins keep working | reveals which of that person's rows share a value |
| nothing matches on it | randomised encryption | reveals nothing | can't join or group on it |
| millions of people | one data key each, in a vault, wrapped by a KMS key | KMS destroys on a schedule (30 days default, 24h minimum) and per-key cost adds up | you run the vault, and it must never be backed up |
| decryption has to happen in SQL, on BigQuery (not built here) | BigQuery AEAD functions, with keysets in a table | the roles allowed can decrypt in a query | plaintext has already reached the warehouse |

### Checking it worked

| If | Do |
|---|---|
| you want to know the sweep did its job | re-query every target afterwards; don't trust its own delete counts |
| someone could restore a backup or add a table later | re-check old requests on a schedule (`verify-all`), and keep an inventory of every table that holds a person |
| a model could put a person back | a dbt test that fails the build if an erased person appears in Gold |

### What no branch solves

- **A trained model has already learned from the data.** Deleting the feature row stops future
  scoring. The realistic fix is retraining on a schedule, so erased people age out.
- **Third parties.** Anything already sent to a CDP or ad platform needs its own deletion call.
  Encryption protects their copy only if what you sent them was ciphertext.
- **Ciphertext is pseudonymised data until the key is gone for good.** So the key destruction has
  to be irreversible, and real deletion should happen everywhere it's possible.

---

## 2. Schema evolution into BigQuery

Detail and measurements: [`beam/README.md`](../beam/README.md).

### A producer is adding a field. How are you writing to BigQuery?

```
Storage Write API (the current default)
├── withAutoSchemaUpdate on
│   ├── column added before the producer ships ...... measured: 0 blank rows of 30,000
│   └── column added as the producer ships .......... measured: 1,577 blank rows over 15.8s
└── withAutoSchemaUpdate off
    └── every row with the new field fails until the pipeline restarts, measured 30,000 of 30,000

Legacy streaming inserts (insertAll)
└── BigQuery rejects the row and Beam retries it until the schema propagates,
    usually within a few minutes

File loads
└── ALLOW_FIELD_ADDITION lets the load job add the column itself, as long as
    the schema it's given includes the new field
```

Beam only learns about a new column from BigQuery's reply to a write, and `withAutoSchemaUpdate`
forces `ignoreUnknownValues` on. So the gap never fails loudly; the new field just lands blank.

| If | Do | What it costs |
|---|---|---|
| you can't accept a single blank value | add the column ahead, turn on auto-update, add a column listing which fields each message carried, and backfill blanks from the raw table | an extra column and a repair job |
| a few seconds of blanks are acceptable | auto-update on its own | a short gap nobody is told about unless you check |
| you're on Beam older than 2.63 and Dataflow autoscales | upgrade | nothing; older versions dropped new fields on write streams opened after the change |
| the source is Pub/Sub with schemas | commit the new schema revision ahead too | measured: Pub/Sub rejected new-version messages for up to 40s after the commit |
| a topic is quiet while the column is added (not measured here) | expect its first batch after the change may land blank | Beam learns about a column from a write reply, so with no writes it hasn't learned yet |

---

## 3. Access control

Detail: [`governance.md`](governance.md).

### What are you protecting?

```
a whole domain, from a team ................ dataset IAM
a curated subset of a table ................ authorized view
some rows of a table ....................... row access policy
some columns
├── the team has no need for them .......... policy tag, no grant: they are denied
└── the team needs a usable version ........ dynamic masking (needs a Google Cloud organisation)
```

Pick the coarsest option that works. Each step down the list costs more to run.

| If | Do | Because |
|---|---|---|
| the project isn't in an organisation | policy tags will deny, but can't mask | the Data Policy API refuses outside an org |
| dbt rebuilds the table (`--full-refresh`, table materialisation) | apply row policies in a dbt post-hook, not Terraform | `CREATE OR REPLACE TABLE` drops every row policy on the table |
| you're the project owner | give yourself an explicit all-regions grant | there's no owner bypass; anyone not in a policy sees zero rows |
| Gold is masked but Bronze isn't locked down | lock Bronze down first | otherwise the masking is bypassed by reading the raw table |
| a team has no business need for the field | deny rather than mask | masking hides the fact that they reached for personal data at all |
| a team needs to join on an email | `SHA256` masking | deterministic, so joins and `COUNT(DISTINCT)` still work |
| a team needs age bands, not birthdays | `DATE_YEAR_MASK` | keeps the useful part, drops the identifying part |

---

## 4. dbt across many teams

Detail: [`modelling-across-sectors.md`](modelling-across-sectors.md).

### Marketing needs something Finance built

```
is it really a general concept that two or more teams need?
├── yes ............................ move it into core; Data Engineering owns it
└── no, it's genuinely Finance's
    ├── Finance is happy to share it ... Finance makes that one model public, with a contract
    └── it's Finance's working table ... no; it stays private
```

| If | Do | What it costs |
|---|---|---|
| teams need to build their own models safely | dbt groups, with staging private to core | tests that reach into another group have to live in that group |
| one team's model depends on another team's | make it visible in CI, don't block it | a check to maintain |
| a team needs its own release schedule and CI | split into separate projects (dbt Mesh) | cross-project refs need dbt Cloud, plus the coordination cost |
| teams only differ by org chart | keep one project with groups | nothing |
| the core build fails | nothing downstream runs | a delayed report, instead of a report that looks fine and is wrong |
