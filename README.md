# ShopVista E-Commerce

**End-to-End Data Engineering & Analytics Solution**

ShopVista's operational data used to live across disconnected CSV exports and legacy systems, which meant hours of manual reporting and inconsistent numbers across teams. This project replaces that with a single, governed, Azure-native pipeline: raw retail data flows in from source, is refined through a Medallion architecture on Databricks, and lands as business-ready datasets in Power BI.

---

## Table of Contents

- [Architecture](#architecture)
- [Storage Layer](#storage-layer)
- [Compute & Governance](#compute--governance)
- [Medallion Architecture](#medallion-architecture)
  - [Bronze](#bronze--raw-landing-zone)
  - [Silver](#silver--cleansed--conformed)
  - [Gold](#gold--business-aggregates)
- [Semantic Model](#semantic-model)
- [Business Impact](#business-impact)
- [Tech Stack](#tech-stack)

---

## Architecture

The pipeline runs end to end from the ShopVista source system to a governed analytics layer:

**ShopVista System → CSV export → Azure Data Lake Storage (ADLS Gen2) → Access Connector → Azure Databricks (Unity Catalog) → Bronze / Silver / Gold → Power BI**

<img width="8107" height="3420" alt="project_architecture" src="https://github.com/user-attachments/assets/3cddd63a-8957-4ece-9478-ec4d6d03936c" />


---

## Storage Layer

All raw data lands in a single, private ADLS Gen2 storage account (`stgsvadlsdevci001`), with dedicated containers per lifecycle stage:

| Container | Purpose | Access |
|---|---|---|
| `ecommerce-raw-data` | Landing zone for raw ingestion | Private |
| `uc-data` | Governed storage under Unity Catalog | Managed |
| `logs` | Operational logging | Internal |

**Hierarchical Namespace (HNS)** is enabled, turning the account into a true file system rather than a flat blob store — this materially reduces latency on the rename/delete operations common in ETL/ELT workloads.

<img width="1294" height="376" alt="image" src="https://github.com/user-attachments/assets/5aadbbea-b669-41f1-a0cd-7254bd58c3aa" />


---

## Compute & Governance

An **Access Connector** bridges ADLS and Azure Databricks, letting Databricks read raw data without credentials ever being exposed. **Unity Catalog** governs the workspace on top of that — centralized access control, auditing, and lineage across every table and transformation.

<img width="442" height="331" alt="image" src="https://github.com/user-attachments/assets/718e80d6-7f32-427b-bf4e-27b1e87fd6f7" />

---

## Medallion Architecture

Transformation logic in Databricks follows a tiered Bronze → Silver → Gold refinement process, avoiding "garbage-in, garbage-out" by validating at every stage.

<img width="921" height="349" alt="image" src="https://github.com/user-attachments/assets/c0df3572-c108-4065-b3cd-f9c5692cf4a1" />



### Bronze — Raw Landing Zone
Immutable, unedited copy of the source data. Enables idempotent reprocessing any time downstream logic changes.

`ecommerce.bronze`: `brz_brands` · `brz_category` · `brz_customers` · `brz_order_items` · `brz_order_shipments` · `brz_products`

<img width="427" height="395" alt="image" src="https://github.com/user-attachments/assets/b1210aac-74bd-44a2-a8ed-95cf64545f1d" />


### Silver — Cleansed & Conformed
Deduplication, schema enforcement, and joins across fragmented records to produce one consistent, enterprise-level view.

`ecommerce.silver`: `slv_brands` · `slv_category` · `slv_customers` · `slv_order_items` · `slv_order_shipments` · `slv_products`

<img width="318" height="316" alt="image" src="https://github.com/user-attachments/assets/f0b54581-9a30-434d-b2c6-ca66d73675db" />


### Gold — Business Aggregates
Pre-calculated joins and dimensional modelling, purpose-built for sub-second Power BI queries.

`ecommerce.gold` — **Dimensions:** `gld_dim_customers` · `gld_dim_date` · `gld_dim_products`
**Facts:** `gld_fact_daily_orders_summary` · `gld_fact_order_items` · `gld_fact_order_shipments`

<img width="455" height="373" alt="image" src="https://github.com/user-attachments/assets/468a339b-7505-40d1-b692-4e53d333f940" />


---

## Semantic Model

Gold tables feed a Power BI **star schema**: four fact tables (`fact_order_shipments`, `fact_daily_orders_summary`, `fact_order_items`, `fact_order_returns`) connected to three dimensions (`dim_date`, `dim_customers`, `dim_products`) via one-to-many relationships — minimizing redundancy and keeping cross-filtering fast at scale.

<img width="865" height="623" alt="Data model" src="https://github.com/user-attachments/assets/94165180-84e8-4876-8fe4-ddd5b53a711a" />


---

## Business Impact

The E-Commerce Analytics dashboard, built on top of this pipeline, surfaces:

- **Total Sales:** £22.19bn across **1.6M units sold** and **299.7K total customers**
- **Repeat Customer Rate:** 66.53%
- **Channel Split:** Website 55% (£12bn) · Mobile 45% (£10bn)
- **Top Brands:** AcmeTech £2.16bn · ByteMax £2.02bn
- **Top Region:** South (75K customers), ahead of West (65K) and North (49K)

<img width="1292" height="718" alt="ecommerce_analytics_report" src="https://github.com/user-attachments/assets/033bd0a0-235b-461f-b55d-5e54a39cf06a" />


---

## Tech Stack

- **Storage:** Azure Data Lake Storage Gen2 (HNS-enabled)
- **Compute / ETL:** Azure Databricks
- **Governance:** Unity Catalog
- **Modelling & Reporting:** Power BI (Star Schema)

