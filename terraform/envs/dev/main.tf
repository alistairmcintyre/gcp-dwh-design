# ------------------------------------------------------------------------------------------------
# Dev environment root: the governed BigQuery lakehouse.
#
# Read this file top to bottom to understand the access model; the modules are mechanism, this is
# policy. Applying it gives you a project where the same query returns different data to different
# identities, which `scripts/validate_governance.py` then proves.
# ------------------------------------------------------------------------------------------------

module "services" {
  source     = "../../modules/services"
  project_id = var.project_id

  services = [
    "bigquery.googleapis.com",
    "bigquerydatapolicy.googleapis.com",
    "datacatalog.googleapis.com",
    "dataplex.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
  ]
}

# ---- Medallion datasets -------------------------------------------------------------------------
module "bigquery" {
  source     = "../../modules/bigquery"
  project_id = var.project_id
  location   = var.region
  labels     = var.labels

  # Demo project: allows `terraform destroy` to clean up fully. Never set true where real data lives.
  delete_contents_on_destroy = true

  # No default expiration: billing is enabled, so BigQuery is out of sandbox mode. (In sandbox both
  # of these are forced to 60 days and Terraform must declare them to match -- see the module vars.)

  datasets = {
    raw = {
      description = "BRONZE. Landed source data, as received, unmodelled. Data Engineering only."
      layer       = "bronze"
    }
    staging = {
      description = "SILVER. Cleaned and conformed 1:1 views over Bronze. Engineers and modellers."
      layer       = "silver"
    }
    marts = {
      description = "GOLD. Contracted, documented, governed business models. The self-serve surface."
      layer       = "gold"
    }
    seeds = {
      description = "Reference data loaded from version-controlled CSVs by dbt seed."
      layer       = "silver"
    }
    elementary = {
      description = "Elementary observability tables: run results, test results, freshness history."
      layer       = "platform"
    }

    # One dataset per domain, not merely one folder. BigQuery grants read access per dataset, so
    # this is what lets Finance's analysts see Finance's models and the shared Gold tables, and
    # nothing else. A single `marts` dataset would make that impossible to express.
    marts_finance = {
      description = "GOLD. Finance domain models. Owned by Finance Analytics."
      layer       = "gold"
    }
    marts_compliance = {
      description = "GOLD. Compliance domain models. Built by DE, owned by Compliance Analytics."
      layer       = "gold"
    }
    features = {
      description = "Feature tables for online serving via Vertex AI Feature Store. No PII."
      layer       = "gold"
    }

    # Holds the materialised masked derivative of dim_client. It exists because an authorized view
    # bypasses table IAM but NOT column policy tags, so the obvious masking-view design fails for
    # exactly the principals it is meant to serve -- see docs/governance.md. The personas module
    # already grants read on this dataset, so it must be declared here or Terraform plans to
    # destroy it on every apply.
    marts_secure = {
      description = "GOLD. Masked derivatives for principals without Fine-Grained Reader."
      layer       = "gold"
    }
  }

  depends_on = [module.services]
}

# ---- Data classification and masking ------------------------------------------------------------
module "governance" {
  source     = "../../modules/governance"
  project_id = var.project_id
  region     = var.region

  taxonomy_name = "pii-classification"

  # Dynamic data masking requires the project to belong to a Google Cloud organization -- the Data
  # Policy API refuses otherwise, on both the v1 and v2 endpoints and with IAM data governance tags
  # as well as Data Catalog policy tags, even with billing enabled. This project sits inside
  # organizations/384677309426, so masking is available.
  #
  # With it OFF, policy tags still enforce -- an ungranted principal has the query REJECTED on the
  # tagged column. With it ON, that principal instead receives a MASKED value, which is what lets an
  # analyst who only needs aggregates keep working rather than being blocked outright.
  enable_data_masking = true

  # The keys here are the contract with dbt: a column's `meta.pii_class` must match one of them.
  # The masking rule is chosen per class from what analysis still needs to be possible on the
  # masked value -- masking is not "hide it", it is "keep it useful and stop it identifying anyone".
  pii_classes = {
    person_name = {
      display_name = "pii-person-name"
      description  = "Given and family names. Direct identifier; no analytical use in masked form, so it is blanked."
      # DEFAULT_MASKING_VALUE returns "" for STRING. Chosen over SHA256 because a hashed name adds
      # re-identification risk (small value space, easy rainbow table) for no analytical benefit.
      masking_rule = "DEFAULT_MASKING_VALUE"
    }
    date_of_birth = {
      display_name = "pii-date-of-birth"
      description  = "Date of birth. A KYC identity attribute and a strong re-identifier when combined with a name."
      # DATE_YEAR_MASK truncates to 1 January of the birth year: age-band and cohort analysis still
      # work, while the exact date -- the part that identifies a person -- is gone. Strictly better
      # than nulling it, which would break every age-banded report and drive analysts to copy the
      # raw data somewhere ungoverned.
      masking_rule = "DATE_YEAR_MASK"
    }
    contact = {
      display_name = "pii-contact"
      description  = "Email address and other contact details."
      # SHA256 is deterministic, so masked email still joins to itself across tables and still
      # supports COUNT(DISTINCT) -- the two things marketing analytics actually needs -- without
      # exposing an address anyone could send mail to.
      masking_rule = "SHA256"
    }
  }

  # RAW access, granted per class rather than as one blanket "can see PII" role. Compliance needs
  # identity attributes to do KYC and regulatory reporting. Marketing needs the email to push
  # audiences to the CDP, and has no business reason to see names or dates of birth -- so it gets
  # exactly one class, and that grant is legible as such in an access review.
  #
  # The build identity appears in every class. Worth being able to defend, because it is the most
  # privileged grant in this file.
  #
  # It is no longer strictly REQUIRED: with native masking, nothing in the pipeline has to read raw
  # PII in order to protect it -- `dim_client` is built from untagged Silver columns, the tags are
  # applied afterwards as a schema patch, and masking happens at read time in the engine. That is a
  # real security gain over materialising a masked copy, which needed an identity that could read
  # everything.
  #
  # It is kept here because this environment collapses two roles into one person: the operator is
  # also the build identity, and the operator needs raw access to act as the unrestricted baseline
  # in `scripts/validate_governance.py`. In production these are separate identities and the dbt
  # service account would hold it only if a model actually reads a tagged column -- at which point
  # the grant becomes required again, and auditable as such.
  fine_grained_readers = {
    person_name   = [module.personas.members["compliance"], var.build_identity_member]
    date_of_birth = [module.personas.members["compliance"], var.build_identity_member]
    contact       = [module.personas.members["compliance"], module.personas.members["marketing"], var.build_identity_member]
  }

  # MASKED access for everyone else who queries the table. A principal with neither grant cannot
  # query the column at all -- the query is rejected. That is deliberate: failing loudly beats
  # returning a column of nulls that someone downstream mistakes for missing data.
  masked_readers = {
    person_name   = [module.personas.members["uk_desk"], module.personas.members["marketing"]]
    date_of_birth = [module.personas.members["uk_desk"], module.personas.members["marketing"]]
    contact       = [module.personas.members["uk_desk"]]
  }

  depends_on = [module.services]
}

# ---- Personas -----------------------------------------------------------------------------------
module "personas" {
  source     = "../../modules/access_personas"
  project_id = var.project_id

  personas = {
    uk_desk = {
      account_id    = "persona-uk-desk"
      display_name  = "Persona: UK desk analyst"
      description   = "UK trading desk. Masked PII, and only rows for the UK regulated entity."
      project_roles = ["roles/bigquery.jobUser"]
    }
    marketing = {
      account_id    = "persona-marketing"
      display_name  = "Persona: marketing analyst"
      description   = "Marketing. Raw email for CDP activation; names and DOB masked; all regions."
      project_roles = ["roles/bigquery.jobUser"]
    }
    compliance = {
      account_id    = "persona-compliance"
      display_name  = "Persona: compliance / KYC analyst"
      description   = "Compliance. Raw PII across all regions for KYC and regulatory reporting."
      project_roles = ["roles/bigquery.jobUser"]
    }
    # Deliberately granted NEITHER fine-grained nor masked reader on any class, which makes it the
    # only persona whose queries are outright REJECTED on a tagged column.
    #
    # Why deny rather than mask, when masking is available? Because masking says "you may see a safe
    # version", and for a team with no business need for customer identity the honest answer is "you
    # may not query this column at all". It also keeps accidents visible: if every principal gets a
    # masked value, `select *` always succeeds and nobody ever learns they were reaching for PII.
    # Denial surfaces that, loudly, at the moment it happens.
    quants = {
      account_id    = "persona-quants"
      display_name  = "Persona: quantitative analyst"
      description   = "Quants. Behavioural and financial columns only; no business need for customer identity, so PII columns are denied outright rather than masked."
      project_roles = ["roles/bigquery.jobUser"]
    }
  }

  # Gold only. None of these personas can read Bronze or Silver, where the same columns sit
  # untagged -- column-level security on the Gold table is worthless if the raw table is readable.
  # This is the most common way a masking implementation is defeated in practice.
  #
  # One dataset is enough because masking happens IN PLACE: the same table serves all three
  # personas and returns raw, masked or nothing depending on the caller's grants. No masked copy,
  # no second dataset, no duplicated PII.
  dataset_readers = {
    uk_desk    = [module.bigquery.dataset_ids["marts"]]
    marketing  = [module.bigquery.dataset_ids["marts"]]
    compliance = [module.bigquery.dataset_ids["marts"]]
    # Table access, but no tag grants. Proves the two are independent: being allowed to read the
    # TABLE says nothing about being allowed to read a governed COLUMN in it.
    quants = [module.bigquery.dataset_ids["marts"]]
  }

  impersonators = [var.operator_member]

  depends_on = [module.bigquery]
}

# ---- Data quality scans -------------------------------------------------------------------------
data "google_project" "this" {
  project_id = var.project_id
}

# Dataplex runs scans as its own service agent, not as the caller. That agent is NOT created by
# enabling the API -- it is created on first use, which means a fresh `terraform apply` fails with
# "The Dataplex service account ... was not found" and only succeeds on the second run. Forcing it
# into existence here makes the stack apply cleanly from nothing, which is the difference between
# infrastructure you can rebuild and infrastructure you can only patch.
resource "google_project_service_identity" "dataplex" {
  provider = google-beta
  project  = var.project_id
  service  = "dataplex.googleapis.com"

  depends_on = [module.services]
}

# Creating the service agent and using it in an IAM binding are not atomic: the account exists
# immediately but IAM rejects it as a member for a short window, with the same misleading
# "Service account ... does not exist" error as if it had never been created. A sleep is the
# unglamorous but correct fix -- the alternative is a stack that only applies on the second run,
# which is not infrastructure you can rebuild.
resource "time_sleep" "dataplex_identity_propagation" {
  depends_on      = [google_project_service_identity.dataplex]
  create_duration = "45s"
}

locals {
  dataplex_agent = "serviceAccount:${google_project_service_identity.dataplex.email}"
}

module "data_quality" {
  source     = "../../modules/dataplex_quality"
  project_id = var.project_id
  region     = var.region

  scans = {
    # BRONZE. The most valuable scan in the set, because nothing else is watching this table: dbt
    # source freshness checks its age, but no dbt test runs against raw data before it is modelled.
    # A source that starts emitting nulls or a new country code shows up here first.
    "bronze-users" = {
      dataset          = module.bigquery.dataset_ids["raw"]
      table            = "users"
      description      = "Bronze landing quality for raw.users -- watches the source contract before any modelling."
      cron             = "0 */6 * * *"
      non_null_columns = ["user_id", "registration_time", "country"]
      unique_columns   = ["user_id"]
      set_rules = [
        {
          column = "country"
          values = ["GB", "IE", "DE", "ES", "BR"]
        },
      ]
      freshness = {
        column        = "_loaded_at"
        max_age_hours = 24
      }
    }

    # GOLD. These duplicate a handful of dbt tests on purpose: dbt proves the table was correct when
    # it was BUILT, this proves it is still correct now. They diverge whenever something writes to
    # the table outside the pipeline, which is exactly the case worth catching.
    "gold-dim-customer" = {
      dataset     = module.bigquery.dataset_ids["marts"]
      table       = "dim_client"
      description = "Gold customer dimension: key integrity, region validity, and KYC completeness."
      cron        = "0 7 * * *"
      # date_of_birth is a KYC identity attribute -- a customer record without one is a compliance
      # gap, not just a data gap, so its completeness is monitored rather than merely tested.
      non_null_columns = ["client_id", "trading_region", "date_of_birth"]
      unique_columns   = ["user_id"]
      set_rules = [
        {
          column = "trading_region"
          values = ["UK", "EMEA", "APAC", "US", "OTHER"]
        },
      ]
      range_rules = [
        {
          column    = "lifetime_stake"
          min_value = "0"
        },
      ]
      row_conditions = [
        {
          column      = "date_of_birth"
          expression  = "DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR) >= 18"
          description = "every customer is at least 18 -- an underage account is a regulatory breach, not a data defect"
        },
      ]
    }

    "gold-fct-user-activity" = {
      dataset     = module.bigquery.dataset_ids["marts"]
      table       = "fct_client_activity"
      description = "Gold daily activity fact: grain integrity and money-column sanity on recent partitions."
      cron        = "30 7 * * *"
      # Scan only the recent partitions. Re-scanning years of immutable history every morning costs
      # real money and can only ever tell you something you already knew.
      row_filter       = "activity_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
      non_null_columns = ["activity_date", "user_id"]
      range_rules = [
        { column = "bet_count", min_value = "0" },
        { column = "total_stake", min_value = "0" },
        { column = "deposit_amount", min_value = "0" },
      ]
      row_conditions = [
        {
          expression  = "trading_revenue = spread_revenue + commission + funding_charge"
          description = "Trading revenue reconciles to spread + commission + funding -- the definition finance signs off"
        },
      ]
    }
  }

  depends_on = [
    module.bigquery,
    google_project_iam_member.dataplex_bq,
    google_data_catalog_policy_tag_iam_member.dataplex_dob_reader,
  ]
}

# Dataplex's service agent must be able to read the tables it scans, and -- for the KYC completeness
# rule on `date_of_birth` -- to read a policy-tagged column. That grant is deliberate and narrow: it
# is the ONE class Dataplex needs, and the scans that touch it must never enable failing-row export,
# which would copy the offending rows into an untagged results table and undo the tagging entirely.
resource "google_data_catalog_policy_tag_iam_member" "dataplex_dob_reader" {
  policy_tag = module.governance.policy_tag_ids["date_of_birth"]
  role       = "roles/datacatalog.categoryFineGrainedReader"
  member     = local.dataplex_agent

  depends_on = [time_sleep.dataplex_identity_propagation]
}

resource "google_project_iam_member" "dataplex_bq" {
  for_each = toset(["roles/bigquery.dataViewer", "roles/bigquery.jobUser"])

  project = var.project_id
  role    = each.value
  member  = local.dataplex_agent

  depends_on = [time_sleep.dataplex_identity_propagation]
}
