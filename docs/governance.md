# Data governance on BigQuery: PII, KYC and regional access

How this repo restricts who can see which rows and which columns, why each control was chosen, and
what deploying it for real taught.

Everything below is deployed and verified in a project inside a Google Cloud
organization, with native dynamic data masking enabled. `scripts/validate_governance.py`
impersonates each persona, runs the queries an analyst would, and asserts the values that come back.
25 checks, non-zero exit on any mismatch, so it runs as a CI gate after a governance change.

```
PERSONA: uk_desk  --  UK trading desk: no PII, UK rows only
  [PASS] Bronze (raw.clients) is not readable
  [PASS] Row access policy returns the expected regions       saw ['UK'], expected ['UK']  (UK=199)
  [PASS] Column-level: person_name is masked                  all 398 name values blanked (DEFAULT_MASKING_VALUE)
  [PASS] Column-level: date_of_birth is masked                truncated to year (DATE_YEAR_MASK), e.g. 1998-01-01
  [PASS] Column-level: contact (email) is masked              SHA256 digest (199 distinct of 199 -- joins preserved)
```

```bash
make governance-apply     # terraform apply
make governance-build     # dbt build with the policy tag ids injected
make governance-validate  # prove it, as each persona
```

---

## 1. The access ladder

Use the coarsest control that satisfies the requirement, and escalate only when it cannot. Each rung
costs more to operate than the one above it.

| Rung | Control | Answers | Cost of using it |
|---|---|---|---|
| 1 | **Dataset IAM** | "can this team see this domain at all?" | almost none; visible in one place |
| 2 | **Authorized views / datasets** | "can they see a curated projection instead of the base table?" | an extra object to keep in step |
| 3 | **Row access policies** | "same table, which rows?" | invisible to dbt and to lineage; dropped by `CREATE OR REPLACE` |
| 4 | **Column policy tags** | "which fields within a row?" | a taxonomy to govern; tags to keep attached |
| 5 | **Dynamic data masking** | "can they see a *safe version* of the field?" | needs an org; a rule per class to justify |

### When row-level security earns its place

Rung 3 gets reached for too early, so it is worth being explicit about when it pays. The platform
modelled here has separately regulated entities in different regions: a UK entity under the FCA, an
EMEA entity, an APAC entity. "An APAC analyst must not see UK client rows" is then a regulatory and
GDPR Chapter V constraint rather than a preference, and one Gold table can serve every desk at once.
The alternative is a copy per region: N pipelines, N chances to drift, N things to re-certify.

Where the split follows table lines instead (Finance never needs marketing tables), rung 1 is the
right answer and row-level security is over-engineering.

---

## 2. Who owns what

| Layer | Owns | Why there |
|---|---|---|
| **Terraform** | taxonomy, policy tags, masking data policies, IAM bindings, personas | slow-moving, org-wide, security-reviewed |
| **dbt** | which columns carry which classification, which rows each group reads | moves at the speed of the data model |
| **`scripts/governance_vars.py`** | joins the two | so no environment-specific id ever appears in a model |

The models declare what the data is. The platform declares what that classification means in this
environment:

```yaml
# dbt/models/marts/core/_core__models.yml, environment-independent, reviewed by an analytics engineer
- name: email
  meta: { pii_class: contact }
```
```hcl
# terraform/envs/dev/main.tf, environment-specific, reviewed by whoever owns security
contact = { display_name = "pii-contact", masking_rule = "SHA256" }
```

`terraform output` → `governance_vars.py` → `dbt --vars` resolves one to the other at build time.
A model file is portable across projects; a taxonomy id never leaks into version-controlled SQL.

### Why row access policies are dbt's job, not Terraform's

`CREATE OR REPLACE TABLE`, which is what dbt issues for a table materialization and for
`--full-refresh`, drops every row access policy on the table. If Terraform owned them, every full
refresh would leave a production table unprotected until the next apply, and Terraform would report
drift on every build. Whoever recreates the table has to reapply the policy in the same breath, so
the definitions live in Terraform outputs (reviewable next to the IAM they complement) and are
applied by a dbt post-hook
(`dbt/macros/row_access_policies.sql`). That macro drops-all-then-recreates, so a policy deleted from
config is actually removed rather than lingering.

---

## 3. The personas

| Persona | Rows | `person_name` | `date_of_birth` | `contact` (email) |
|---|---|---|---|---|
| **UK desk analyst** | `trading_region = 'UK'` only | denied | denied | denied |
| **Marketing analyst** | all regions | denied | denied | **raw** |
| **Compliance / KYC** | all regions | **raw** | **raw** | **raw** |
| **Quants** | all regions | *denied* | *denied* | *denied* |
| **dbt build identity** | all regions | **raw** | **raw** | **raw** |

Quants shows the third outcome. It holds dataset access and an all-regions row grant but no tag
grant of any kind, so a query against a governed column is rejected rather than masked. Denying is
the right answer for a team with no business need for customer identity. Masking would hide the
mistake: if every principal gets a masked value back then `select *` always succeeds and nobody finds
out they were reaching for PII in the first place.

Raw access is granted per class, not as one blanket "can see PII" role. Compliance needs identity
attributes for KYC and regulatory reporting. Marketing needs the email to push audiences to the CDP
and has no reason to see names or dates of birth, so it gets exactly one class, and the grant reads
that way in an access review.

In production these are Google Groups, never individuals and never service accounts. Joiners and
leavers then become an identity-team operation rather than a Terraform pull request, and an access
review has one object to read per persona. Service accounts are used here only because they can be
impersonated, which is what makes the model testable.

---

## 4. Masking rules, and why each one

Masking is not "hide it", it is "keep it useful and stop it identifying anyone". A control that
blocks legitimate analysis gets routed around, and the route is usually an ungoverned copy in a
spreadsheet.

| Class | Rule | Reasoning |
|---|---|---|
| `person_name` | `DEFAULT_MASKING_VALUE` (`''`) | a hashed name buys nothing analytically and adds re-identification risk, since the value space is small and trivially rainbow-tabled |
| `date_of_birth` | `DATE_YEAR_MASK` | age banding and cohort analysis survive; the exact date, which is the identifying part, does not. Strictly better than nulling, which breaks every age-banded report |
| `contact` | `SHA256` | deterministic, so a masked email still self-joins and still supports `COUNT(DISTINCT)`, which is what marketing analytics needs, without exposing an address |

A policy tag can carry several data policies with different principals on each. Marketing gets
`SHA256` (still matchable to the CDP) while Support gets `EMAIL_MASK` (can see the domain to
troubleshoot). One per class here so the demonstration has one unambiguous answer per identity.

---

## 5. What deploying it taught

All of these came out of running it rather than reading documentation, and each one changed the design.

**Data masking requires the project to belong to an organization.**
```
Error 400: The project 1041708020621 needs to belong to an organization to manage DataPolicies.
```
Checked against the v1 API, the v2 API, and with IAM data governance tags rather than Data Catalog
policy tags. The constraint is on creating any data policy at all, and enabling billing does not lift
it. Taxonomy and policy tags still work without an org, and they still enforce: an ungranted
principal has the query rejected on that column.

The stack was then redeployed into a project inside a real organization with
`enable_data_masking = true`, and native masking works as configured. Same Terraform, one variable
different. So both behaviours are verified: deny without an org, mask with one.

While masking was unavailable the fallback was a materialised masked copy of the Gold table. That
copy has since been deleted. A masked copy needs a build identity that can read all raw PII in order
to write the masked version, and it leaves a second copy of PII-derived data on disk. Native masking
needs neither, because the tags are a schema patch and the engine masks at read time, so switching
removed both a privileged grant and an extra copy. That is a security argument for masking in place,
not just a convenience one.

**BigQuery's `SHA256` masking rule returns base64, not hex.**
```
'MpmZ1do1joVTT9bRQStBtlZ7QDgSUBt50XlYWMmUPDs='   44 chars, '=' padded
```
Only visible because the validation asserts on the value rather than on the query succeeding. A
check that confirms "the query worked" passes just as happily against completely unmasked data.
Distinctness survives (199 rows, 199 distinct digests), which is the property that matters here: it
is why `SHA256` was chosen over nulling the column, since marketing can still join and
`COUNT(DISTINCT)` on it and has no reason to route around the control.

**Deny, not silent omission.** A principal with neither Fine-Grained Reader nor Masked Reader gets
`403 Access Denied ... on column`, not a row with that field missing. Design around it: an analyst
cannot mistake a withheld value for a genuine null, and an unauthorised export cannot happen
quietly.

**Project owner does not bypass column-level security.** The dbt build failed with *"User has
neither fine-grained reader nor masked get permission"* while running as project Owner. The build
identity needs an explicit grant on every class it reads. That grant is the most privileged thing in
the Terraform. In production it belongs to a dedicated service account whose runs are logged, not to
a human whose console session is not.

**There is no owner bypass for row-level security either.** The moment one row access policy exists
on a table, an identity named in no policy sees zero rows, including the platform owner. The
operator needs an explicit all-regions grant or their own queries silently return nothing.

**Authorized datasets bypass table IAM but not column policy tags.** While building that fallback the
obvious design was a view in an authorized dataset. It fails for exactly the principals it exists to
serve, because BigQuery evaluates policy tags against the calling principal, so a view that selects
`first_name` in order to mask it is denied. SQL-level masking therefore has to be materialised by an
identity holding fine-grained reader: a second copy, a refresh lag, and masking fixed at build time
rather than per role. That is what you are left with on a warehouse without native masking.

**A service agent exists before IAM will accept it as a member.** `google_project_service_identity`
creates the Dataplex agent, and the immediately-following IAM binding fails with
*"Service account service-...@gcp-sa-dataplex.iam.gserviceaccount.com does not exist"*, while the
API confirms it does. It is propagation lag behind a misleading error. A `time_sleep` fixes it;
without one the stack only applies on the second run.

**Dataplex scans cannot be created before their target table exists.**
```
Error 400: The source BigQuery table of the data scan is not found: [dataset:marts, table:dim_client]
```
The Gold tables are built by dbt, which needs the datasets and policy tags Terraform creates, so the
real dependency is infra, then data, then infra again. `make verify-cloud` runs two applies. A
single-apply story only works on a project where the tables already exist, which a fresh one never
does.

**BigQuery sandbox forces a 60-day default expiration** on every dataset and rejects any update that
removes it, so Terraform has to declare the same value or every plan shows permanent drift. Delete
that line the moment billing is enabled: a default expiration on a warehouse dataset silently deletes
tables 60 days later.

**Dataplex's service agent is created on first use, not by enabling the API.** A fresh `terraform
apply` fails with *"The Dataplex service account ... was not found"* and only succeeds on the second
run. `google_project_service_identity` forces it into existence, which is the difference between
infrastructure you can rebuild and infrastructure you can only patch.

---

## 6. The bypass that matters most

Column-level security on a Gold table is worthless if the Bronze table holding the same columns
untagged is readable. That is the most common way a masking implementation gets defeated, so it is
the first thing the validation checks, before rows and before columns:

```
[PASS] Bronze (raw.clients) is not readable
       query denied -- 403 Access Denied: Table <project>:raw.clients
```

No persona can read Bronze or Silver. Only Gold, and only through the controls above.

---

## 7. Rollout, in a real organisation

1. **Classify before enforcing.** Run Sensitive Data Protection (DLP) discovery to find PII you did
   not know about. The columns you know about are not the risk.
2. **Tag in monitor-only mode first.** Policy tags can be applied without enforcement, so you can
   see who *would* have been denied via audit logs before anyone is.
3. **Read the audit logs, then fix the grants**, not the other way round. Enforcing first means
   discovering your access model through incidents.
4. **Enforce, with the masked alternative already in place.** Denying access without offering a
   governed alternative is how the ungoverned spreadsheet copy gets created.
5. **Keep the validation in CI.** Access controls drift. `validate_governance.py` runs on every
   change to the governance stack and fails the build if a persona's reality stops matching its
   documented entitlement.
