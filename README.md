# Excel‑to‑SQL Lead ETL (SQL Server)

This repo documents the **exact workflow I built and used at work** to turn messy Excel lead sheets into a single, reliable dataset for reporting and analysis.


## What this workflow does (end‑to‑end)
- **Extracts** lead rows from incoming Excel files into a staging table (`raw_leads`).  
- **Normalizes** key fields in a view (`raw_leads_normalized`): trims names/phones, lower‑cases emails, cleans property type/status/source, parses dates, and converts **price ranges** (₹, “L”, commas, “to” / “-”) into a numeric **budget**.  
- **Validates** required details in a second view (`required_details`) and **quarantines** the missing-data rows to `leads_rejected` for agent fixes.  
- **Removes** invalids from the active pipeline so only good data proceeds.  
- **De‑duplicates** on a stable key (lead ID) using `ROW_NUMBER()` and inserts the first/clean record into the curated table (`master_leads`).  
- **Publishes** one clean, consistent dataset ready for dashboards and follow‑up actions.

> The entire flow is captured in a single script: `Workflow.sql` (DDL + DML). I ran it in SSMS and later scheduled it via SQL Server Agent for daily refreshes.


## Objects created by the script
- **Tables**: `raw_leads`, `leads_rejected`, `master_leads`  
- **Views**: `raw_leads_normalized`, `required_details`

> The script also includes helper updates to stamp `source_file` and `agent_name` after each import so every row has lineage.


## How I ran it (runbook)
1. **Import Excel → raw**: Load each new Excel file into `raw_leads` (using OPENROWSET/ACE or a manual import).  
2. **Stamp lineage**: Update `source_file` and `agent_name` (e.g., `agent1_leads.xlsx` → Vishal, etc.).  
3. **Normalize**: Use `raw_leads_normalized` to standardize phones/emails/dates and derive `budget` from `price_range`.  
4. **Validate & quarantine**: `required_details` picks rows missing essentials; insert them into `leads_rejected`.  
5. **Clean pipeline**: Remove rejected rows from further processing.  
6. **De‑dup & publish**: Insert the first record per `lead_id` into `master_leads`.  
7. **(Optional) Export rejects**: Send `leads_rejected` back to agents for fixes and re‑ingest later.  



## Data quality rules I enforced
- **Phones/Emails**: trimmed and standardized; emails lower‑cased.  
- **Dates**: parsed into `DATE` (both enquiry and sale dates).  
- **Budget**: parsed from `price_range` by removing ₹, commas, blank spaces; translating “L” to zeroes; handling ranges written as `min‑max` or `min to max`.  
- **Required fields**: name, phone, date, property type, budget — missing rows are quarantined with a reason.  
- **Deduplication**: `ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY lead_id)` and keep the first clean record.


## Outcome (why this mattered at work)
- One **source of truth** for daily dashboards and follow‑ups.  
- **Quicker fixes** via a dedicated rejected‑leads list with reasons.  
- **Cleaner data** thanks to standardization + dedupe, reducing double‑calls and report noise.  
- **Repeatable**: the same script runs safely every day (or on demand) in SQL Server.


