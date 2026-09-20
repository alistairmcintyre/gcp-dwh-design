# Erasure requests: crypto shredding and Kafka tombstones

How a "delete everything you hold about me" request is actually carried out across a streaming
estate and a warehouse, and what each mechanism can and cannot promise.

The hard part is not the delete statement. It is that personal data has been copied: into Kafka logs
with weeks of retention, into Bronze, into models built from Bronze, into feature tables, into
backups, and sometimes out to a third party. A request has to reach all of it within a month, and
you have to be able to show that it did.

## What the law actually asks for

| Article | What it means here |
|---|---|
| 17, right to erasure | Delete the personal data, unless a listed exemption applies |
| 17(3)(b), legal obligation | Records you are required to keep survive the request. Transaction and AML records do |
| 12(3), one month | The clock starts when the request arrives, not when you notice it |
| 18, restriction of processing | Stop using the data now, even before the deletion finishes |

Article 18 is the one people miss, and it is the cheapest to satisfy: stop processing immediately,
then take the time you need to delete properly.

## The two mechanisms

### Tombstones, for the log

A tombstone is a record with the subject's key and a `null` value on a compacted topic. Compaction
removes every earlier record for that key, then removes the tombstone after `delete.retention.ms`.

It works only where the topic is **keyed by the subject and compacted**. A trade topic is keyed by
trade id for throughput, so no tombstone can target a person in it.

The trap is timing. Compaction only touches closed segments, and by default waits until half the log
is uncompacted, so a low-volume topic can sit on a tombstone for weeks and quietly miss the deadline.
Anything claiming `erasure: tombstone` in `streaming/topics.yaml` therefore has to pin
`max_compaction_delay_hours`, and CI rejects it if it doesn't.

The second half matters as much as the first: the tombstone is the **signal to every consumer** that
the record is gone. A Bronze loader that treats a null payload as a corrupt message will erase the
topic and leave the warehouse untouched, which is the worst possible outcome. The generated offload
jobs turn a tombstone into a delete marker row instead:

```yaml
_is_delete: event_id is null          # key present, payload gone
_event_time: coalesce(event_timestamp, kafka_timestamp)
```

### Crypto shredding, for everything you cannot rewrite

Each subject gets their own data key. Personal fields are encrypted with it at the producer, so no
plaintext ever reaches Kafka or the warehouse. Erasing means destroying the key: the ciphertext stays
where it is and stops meaning anything, everywhere, at once.

This is the only mechanism that reaches:

- Kafka segments still inside retention on topics that cannot be compacted
- backups and snapshots, which by design cannot be edited
- extracts already sent to a partner
- records you are obliged to keep, where the transaction survives but the identity on it does not

Say the caveat out loud: **encrypted data is pseudonymised, not anonymised**, until the key is gone.
The erasure claim rests entirely on that destruction being irreversible, which means the vault has no
backups, no soft delete, and no key-version recovery window. Cloud KMS's own `destroy` has a
scheduled delay for exactly this reason, which is why the per-subject key here is a row in a vault
you control, wrapped by a KMS key, rather than a KMS key per person.

Encryption mode is a real choice, not a detail:

| Mode | Behaviour | Use it when |
|---|---|---|
| Randomised (default) | Same value encrypts differently each time | Nothing downstream joins on the field |
| Deterministic | Equal values give equal ciphertext, so joins survive, and equality leaks | A downstream system genuinely has to match on it, such as an email for audience matching |

The deterministic nonce is derived under the subject's own key, so the same email for two different
people still encrypts differently. Equality only leaks within one person's own rows.

## How a request flows through this repo

```
request lands in raw.erasure_requests
        │
        ├─ next dbt build: stg_clients drops the subject          (Article 18, minutes)
        │
        └─ nightly sweep, privacy/erasure.py
               1. destroy the data key                            (crypto shred, immediate)
               2. delete rows listed as `delete` in the inventory (Bronze, Gold, features)
               3. plan and produce tombstones for keyed topics    (log, within the pinned delay)
               4. re-query every delete target and prove it is 0
               5. write completed_at and the row count
```

Step 1 comes first on purpose. If the job dies halfway through, a subject whose key is already
destroyed is unreadable everywhere, which is the safe failure. Deleting first and dying before the
shred leaves readable data with the key still sitting there.

| Layer | What happens | Where |
|---|---|---|
| Kafka | Tombstone on keyed compacted topics; PII fields were encrypted before publishing | `streaming/topics.yaml`, `privacy/tombstones.py` |
| Bronze | Delete markers land from tombstones; rows deleted by the sweep | generated jobs in `spark/jobs/generated/` |
| Silver | Subject filtered out of `stg_clients` on the next build | `dbt/models/staging/stg_clients.sql` |
| Gold and features | Rows deleted by the sweep, absence asserted by a test | `dbt/tests/assert_erased_clients_absent.sql` |
| Key vault | Key row deleted, audit row written with no personal data in it | `privacy/vault.py` |
| Orchestration | Daily, both stacks | `airflow/dags/gdpr_erasure_dag.py`, `dagster/dwh_dagster/privacy_jobs.py` |

## Deleting from Gold is not enough on its own

The one that caught me, and it only showed up on a from-scratch build in CI.

Trades and account transactions are retained under a record-keeping obligation, so they keep the
erased client's rows. The daily activity fact is derived from those retained records and left-joins
the client dimension for attributes. So the sweep deleted the Gold rows, and the very next full
build rebuilt them straight out of the retained trades, with null attributes where the client used
to be. An incremental build hid it locally, because the deleted history was outside the window.

Erasure therefore has to be applied wherever client-grain rows are **derived**, not only where the
client is stored. The filter now sits in `int_client_daily_activity`, so everything downstream
inherits it. The retention obligation covers the transaction record; it does not license rebuilding
a per-client activity profile from it the next morning.

Worth knowing because the same shape appears anywhere a fact outlives its dimension: a warehouse that
can rebuild a person from what it is allowed to keep has not really erased them.

## The inventory is the real artefact

`privacy/erasure_targets.yaml` lists every place a subject appears and what happens to it. It exists
because the question that sinks an erasure process is "are you sure that is all of it?", and a grep
at request time only finds what exists today.

Three actions, and the difference between them is legal rather than technical:

- **delete**: the row exists only because the person does
- **shred**: the row must survive, the identity on it must not
- **retain**: kept under a lawful basis that survives the request, which the entry has to name

Adding a table that carries `client_id` without adding it here shows up in code review, which is the
point of the file being next to the pipelines rather than in a wiki.

## What this design does not solve

Worth being straight about, because an interviewer will ask and a regulator certainly will.

- **A trained model has not forgotten.** Deleting the feature row stops future inference on the
  person, but weights trained on their data still carry it. The practical answer is a retraining
  cadence that ages erased subjects out; the honest answer is that machine unlearning is unsolved.
- **Aggregates stay.** `fct_acquisition_events` holds no client column and every group covers many
  people, so nothing singles anyone out. But historical totals do shift once the underlying rows go,
  and finance will notice, so restatement has to be a decision rather than a surprise.
- **Third parties are not in your control.** A CDP or an ad platform you exported to needs its own
  deletion call. Crypto shredding is what protects the copy they already took, provided you sent
  ciphertext.
- **Backups.** Crypto shredding is the answer. Restoring a backup taken before the erasure will
  otherwise put the subject back, which is why `verify-all` re-checks completed requests rather than
  trusting that the sweep worked once.
- **The pseudonymisation argument.** Some regulators treat ciphertext plus destroyed key as erasure,
  others as pseudonymisation. The defensible position is key destruction that is demonstrably
  irreversible, plus real deletion everywhere deletion is possible. Doing only the crypto half and
  calling it erasure is the weak version of this answer.

## Running it

```bash
# what the queue would do, changing nothing
uv run python -m privacy.cli sweep --dry-run

# process it (DuckDB locally, --target bigquery in the warehouse)
uv run python -m privacy.cli sweep

# prove it, for one subject or for every completed request
uv run python -m privacy.cli verify --subject cli-00000007
uv run python -m privacy.cli verify-all

# how long open requests have been waiting; non-zero exit while there is still time to act
uv run python -m privacy.cli deadlines --warn-days 21

# what the registry allows, and what the inventory says
uv run python -m privacy.cli check-topics
uv run python -m privacy.cli targets
```

Generating data with encryption on, which is how a producer would write it:

```bash
uv run python scripts/generate_test_data.py --encrypt-pii
```

The personal columns arrive as ciphertext, `date_of_birth` moves to `date_of_birth_encrypted` so the
date column keeps its type, and the wrapped keys land in a separate `privacy` schema that must be
left out of every backup. It is off by default because the column-masking demo in
[`governance.md`](governance.md) needs readable values to be worth looking at.

## How this sits next to the access controls

Masking and policy tags answer "who may see this field". Erasure answers "what happens when the
person withdraws". They share one vocabulary: a field classified `pii_class: contact` in a dbt model
is the same field the Terraform taxonomy masks, the same field the topic registry says to encrypt,
and the same field the inventory covers. Classify once, at the point the data enters the estate, and
every layer inherits it.
