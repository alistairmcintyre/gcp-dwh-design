# Building dbt models for many teams at once

How one dbt project can serve Finance, Compliance, Marketing, Risk and the trading desks without
those teams treading on each other, and without Data Engineering becoming the bottleneck.

---

## 1. The problem, in plain words

Picture one data platform, and many teams who all want to use it.

Some teams are good at this. They want to write their own models and not wait for anyone.
Other teams have no data engineers. Data Engineering builds everything for them.

If you let everyone write whatever they like in one project, two bad things happen.

**Bad thing one.** Somebody builds a report on top of a table you were about to change. You change it.
Their report breaks. Nobody knew it was going to.

**Bad thing two.** Three teams each invent their own definition of "an active client". Three reports
disagree. Someone takes all three into a board meeting.

So we need rules. Rules written in a wiki are not rules, they are suggestions. We need rules the
computer enforces.

---

## 2. The idea in one picture

Think of it like a shop.

- **The kitchen** is where Data Engineering works. Nobody else goes in there.
- **The counter** is what the shop sells. It is tidy, labelled, and it does not change without notice.
- **Each team's own table** is where a team takes things from the counter and makes their own meal.

Teams can take anything from the counter. Teams cannot walk into the kitchen. And teams cannot reach
across and grab food off another team's table.

In dbt, those three things have names:

| The shop | In dbt | What it means |
|---|---|---|
| The kitchen | `staging`, `intermediate` | Data Engineering's working area. **Private.** |
| The counter | `marts/core` | The shared, promised tables. **Public.** |
| A team's own table | `marts/finance`, `marts/compliance`, … | That team's own models. **Private to them.** |

---

## 3. How the computer enforces it

dbt has two settings that do this work. They are called **groups** and **access**.

A **group** says who owns a model. We have one group per team: `core`, `finance`, `compliance`,
`marketing`, `risk`, `ml`.

**Access** says who is allowed to use a model. There are three settings:

| Setting | Who can use the model |
|---|---|
| `private` | Only models in the **same group** |
| `protected` | Any model in the **same project** |
| `public` | Anything, including other projects |

We use it like this:

```yaml
# dbt_project.yml
models:
  dwh:
    staging:
      +group: core
      +access: private        # the kitchen
    intermediate:
      +group: core
      +access: private        # also the kitchen
    marts:
      core:
        +group: core
        +access: public       # the counter
      finance:
        +group: finance
        +access: private      # Finance's own table
      compliance:
        +group: compliance
        +access: private
```

Now the important part. If a Finance model tries to use a staging model, dbt refuses to even read
the project. Not a warning, a hard stop:

```
Parsing Error
  Node model.dwh._violation_probe attempted to reference node
  model.dwh.stg_clients, which is not allowed because the referenced
  node is private to the 'core' group.
```

That is the whole point. A written standard can be ignored. This cannot.

When I turned this on it immediately caught two of my own tests reaching into staging. They now say
`{{ config(group='core') }}` because they genuinely are core tests. The rule found a real mistake the
moment it existed.

---

## 4. Which BigQuery datasets exist

Each layer gets its own BigQuery dataset, because BigQuery hands out permissions per dataset.
Separate datasets means Finance's analysts can have access to Finance's tables and the shared tables,
and nothing else.

| Dataset | What is in it | Who can read it |
|---|---|---|
| `raw` | Data exactly as it arrived. Nothing cleaned up. | Data Engineering only |
| `staging` | Cleaned up, one table per source. Views, not tables. | Data Engineering only |
| `marts` | The shared tables: `dim_client`, `fct_client_activity`, `fct_acquisition_events` | Everybody |
| `marts_finance` | Finance's own models | Finance + Data Engineering |
| `marts_compliance` | Compliance's own models | Compliance + Data Engineering |
| `features` | Data shaped for machine learning | Data Science |
| `seeds` | Small lookup lists kept in the repo as CSV files | Everybody |
| `elementary` | Tables the monitoring tool writes | Data Engineering |

Finance and Compliance exist in this repo. Marketing and Risk would be `marts_marketing` and
`marts_risk`, added the same way: a folder, a group, a dataset.

There is no `intermediate` dataset. Those models are ephemeral, so dbt pastes them into whatever uses
them instead of building a real table. They exist to keep the SQL readable, not to
be queried.

---

## 5. What happens if Marketing needs something Finance built?

This is the question that breaks a lot of designs.

Say Marketing wants to build a report. It needs a number that only exists inside a Finance model.
Under the rules above, Marketing cannot use it. dbt will refuse.

Is that the rule failing? Mostly no. It is the rule forcing a conversation that needed to happen.
Which of three situations you are in decides what to do.

### Situation one: the thing is not really a Finance thing

Most of the time, what Marketing wants is something general, like "how much did this client trade last
month". Finance happened to build it first, but it is not a Finance concept.

The answer: move it into `marts/core`. It becomes a shared table, Data Engineering owns it from then
on, and both teams use the same one.

The rule I would write down is that if two or more teams need it, it belongs in core. One team
needing it is a domain model. Two teams needing it is a shared table wearing a disguise.

### Situation two: it really is a Finance thing, and Finance is happy to share it

Sometimes the thing genuinely belongs to Finance. Revenue recognised by regulated entity, say. Finance
understands the rules; Data Engineering does not.

The answer: Finance publishes that one model on purpose. They change it from `private` to `public`
and give it a contract, which is a written promise about the columns, the types and the refresh.

Make the cost visible though. Publishing means Finance can no longer change that model freely. They
have taken on an obligation, and it should feel like a decision rather than a shortcut.

### Situation three: Marketing wants Finance's working-out

Sometimes Marketing wants a half-finished table inside Finance's folder, the equivalent of Finance's
scratch paper.

The answer here is no, and it stays blocked. Depending on somebody else's scratch paper is how you
get woken at 3am because a team you have never met renamed a column.

### An honest limitation

dbt's permissions are not fine-grained. There are three levels: your own group, the whole project, or
everywhere. There is no "share this with Finance and Marketing but nobody else".

So if Finance wants to share one model with Marketing, the clean choices are to make it fully public
or move it to core. I treat "Finance wants to publish something" as a strong hint that it should
become a core table instead.

I would also make cross-team dependencies visible: a small CI check that lists every time one team's
model uses another team's model. Not to block it, but so it gets noticed in review rather than a year
later when someone tries to change it.

---

## 6. Running the build: what has to finish before what

This is the part the folder structure does not tell you.

**You cannot build one team's models on their own.** Marketing's Gold table needs `dim_client`.
`dim_client` needs `stg_clients`. `stg_clients` needs `raw.clients`. So the shared tables have to be
finished before any team starts.

The build runs in three waves.

```
              ┌──────────────────────── wave 1: core ─────────────────────────┐
raw tables ──▶│ stg_* ──▶ int_client_daily_activity ──▶ dim_client, fct_*     │
              └───────────────────────────────┬───────────────────────────────┘
                                              │ tests must pass first
              ┌───────────────────────────────┴───────────────────────────────┐
  wave 2      │  finance        compliance        marketing        risk       │  (in parallel)
              └───────────────────────────────┬───────────────────────────────┘
  wave 3      │  ml features, regulatory extracts, files for other systems    │
              └───────────────────────────────────────────────────────────────┘
```

### Wave 1: the shared foundation

Everything Data Engineering owns. Nothing else can start until this finishes and its tests pass.

```
raw.clients ─┐
raw.trades  ─┼─► stg_clients, stg_trades, stg_orders, stg_account_transactions
raw.orders  ─┤        │
raw.account_transactions      └─► int_client_daily_activity
                                      │
                                      └─► dim_client
                                          fct_client_activity
                                          fct_acquisition_events
```

### Wave 2: every team, at the same time as each other

Once the shared tables are built, the teams run in parallel. They do not depend on each other, so
nothing has to wait.

### Wave 3: things that feed other systems

Machine learning features, regulatory extracts, files sent to other platforms.

### How you actually express that in Airflow

dbt can select models by group, which makes this simple:

```python
wait_for_partitions >> core >> [finance, compliance, marketing, risk] >> ml
```

with each task running one dbt command:

| Task | Command |
|---|---|
| `core` | `dbt build --select group:core` |
| `finance` | `dbt build --select group:finance` |
| `compliance` | `dbt build --select group:compliance` |
| `marketing` | `dbt build --select group:marketing` |
| `risk` | `dbt build --select group:risk` |
| `ml` | `dbt build --select group:ml` |

`--select group:finance` builds only Finance's models. It does not rebuild the shared tables, because
those are already built and the `ref()` calls point at them.

### Why split it into tasks at all, instead of one big `dbt build`?

Four reasons, and they are all about what happens when something goes wrong.

- **You can see who is broken.** If the Finance task is red, it is a Finance problem. One giant task
  going red tells you nothing.
- **One team's failure does not stop the others.** If Compliance fails, Marketing still finishes.
- **You can re-run just the broken bit.** Re-running one team takes a minute. Re-running everything
  takes much longer and costs real money.
- **You can alert the right people.** The Finance task pages the Finance owner, not Data Engineering.

One rule must not bend: if wave 1 fails, nothing in wave 2 runs. Building a Marketing report on top
of a broken `dim_client` is worse than not building it at all, because the report will look fine and
be wrong.

---

## 7. A worked example

Marketing want a table answering: how many clients from each advertising channel actually traded last
month, and how much revenue did they bring in?

Here is what that needs, and in what order.

| Step | Table | Owner | Why it is needed |
|---|---|---|---|
| 1 | `raw.clients`, `raw.trades` | Ingestion | The raw data has to have landed |
| 2 | `stg_clients`, `stg_trades` | Core | Cleaned up, types fixed, one row per thing |
| 3 | `int_client_daily_activity` | Core | Trades summed up per client per day |
| 4 | `dim_client` | Core | One row per client, with their advertising channel on it |
| 5 | `fct_client_activity` | Core | One row per client per day, with revenue |
| 6 | `marts_marketing.fct_channel_performance` | **Marketing** | Marketing groups steps 4 and 5 by channel |

Marketing only write step 6. Steps 1 to 5 already exist and are already tested.

Their model is short, because all the hard work is done:

```sql
{{ config(materialized='table', group='marketing', access='private',
          schema='marts_marketing') }}

with activity as (
    select * from {{ ref('fct_client_activity') }}
),

clients as (
    select * from {{ ref('dim_client') }}
)

select
    a.activity_date,
    c.acquisition_channel,
    count(distinct a.client_id) as trading_clients,
    sum(a.trade_count)          as trades,
    sum(a.trading_revenue)      as trading_revenue
from activity as a
inner join clients as c on a.client_id = c.client_id
group by 1, 2
```

Notice what it uses: `fct_client_activity` and `dim_client`, both shared tables. It cannot reach into
`stg_clients`, and it does not need to.

Notice what that buys Data Engineering. `stg_clients` can be rewritten tomorrow. As long as
`dim_client` still has the same columns, Marketing's model does not care and does not break.

---

## 8. Moving a team from "we build it for them" to "they build it themselves"

A team with no engineers today may want to own its models later, so build them handover-ready from
the start.

When Data Engineering builds models for such a team, they do not go in a special folder. They go in
that team's folder, under that team's group, with that team's name as the owner, exactly as if the
team had written them.

```yaml
# _groups.yml
groups:
  - name: compliance
    description: >
      Compliance models. Built by Data Engineering today, owned by Compliance Analytics.
    owner:
      name: Compliance Analytics
      email: compliance-analytics@example.com
```

Because of the access rules, those models already only use shared tables. They had no choice.

So handing over means changing who owns the group and giving them write access to the repository.

No rebuild, no migration, and no "we will tidy it up when they take it over", which never happens.

---

## 9. One dbt project, or several?

Start with one project and use groups. It is simpler, it works in dbt Core, it needs one CI pipeline,
and it gives you the enforcement that matters.

Split into separate projects, which dbt calls Mesh, only when a team genuinely needs to release on its
own schedule and run its own CI. Referring to a model in another project is a paid dbt Cloud feature.

Splitting early gives you the coordination cost of a distributed system and none of the independence,
because the team still cannot deploy on their own. Split when the release schedule diverges, not when
the org chart does.
