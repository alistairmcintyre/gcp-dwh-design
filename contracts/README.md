# Data contracts

A data contract is the **producer's promise about a dataset**, written down, versioned and enforced
by CI. It is the JD's most-repeated theme, and the thing that makes self-service safe: a division
can build on Core Lake data precisely because someone has committed to what that data will look
like tomorrow.

## What a contract is, and what it is not

A contract is **not** a schema file. A schema says what the columns are today. A contract says:

| Element | Question it answers |
|---|---|
| Schema + types | what are the fields? |
| **Owner** | who do I page at 3am, and who approves a change? |
| **Semantics** | what does `trading_revenue` actually mean, in the finance department's words? |
| **Guarantees** | grain, uniqueness, nullability, referential integrity |
| **SLOs** | how fresh, how complete, how available, with numbers |
| **Compatibility policy** | what may change without asking, and what requires a version bump |
| **Classification** | which fields are personal data, in the vocabulary the platform enforces |
| **Consumers** | who breaks if this changes, the thing nobody can answer without it |

The last two are what make a contract more than documentation. `pii_class` here is the *same*
vocabulary as `meta.pii_class` in the dbt models and the Terraform taxonomy, so classifying a field
in the contract is what causes it to be masked in the warehouse. And a declared consumer list turns
"can we drop this column?" from a guess into a query.

## How it is enforced

Three layers, each catching what the previous cannot:

| Layer | Mechanism | Catches |
|---|---|---|
| **Contract → dbt** | the contract's schema and guarantees are mirrored as dbt `contract: {enforced: true}` + tests | the build produces something the contract does not describe |
| **CI compatibility check** | `services/contract-api` `/validate` diffs a proposed contract against the current one | a breaking change merged without a version bump |
| **Runtime** | Dataplex scans + freshness monitoring against the declared SLOs | the contract is met on paper and violated in production |

A contract nobody checks is a wish. The compatibility check is the part that actually changes
behaviour, because it turns "please don't break consumers" into a failing build.

## Compatibility rules

Modelled on schema-registry semantics, because the same reasoning applies:

| Change | Compatible? | Why |
|---|---|---|
| Add an optional field | yes | existing consumers ignore it |
| Add a required field | **no** | existing writers do not produce it |
| Remove a field | **no** | a consumer may select it |
| Widen a type (`INT64` → `NUMERIC`) | yes | every existing value still fits |
| Narrow a type | **no** | existing values may not fit |
| Relax nullability (required → optional) | **no** | consumers may assume it is present |
| Tighten nullability (optional → required) | yes | consumers already handle the value |
| Change semantics with the same type | **no**, and the dangerous one | nothing detects it; only review does |

The last row is why contracts carry a prose `description` per field and why changing one requires
the owner's approval. A silent redefinition of `trading_revenue` from `spread + commission + funding` to `spread + commission`
passes every automated check ever written and quietly restates the company's revenue.
