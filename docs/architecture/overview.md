# Architecture Overview

**Last updated:** 2026-01-06
**Owner:** Eric Millan
**Status:** **Draft** | Reviewed | Approved

---

## 1. Purpose

This document provides a high-level overview of the system architecture for **Chronice Wasting Disease Reporting**.  
It is intended to help:

- New contributors understand how the system fits together
- Reviewers assess technical design and risk
- Future maintainers understand constraints and trade-offs

This document focuses on **structure and flow**, not implementation details.

---

## 2. System Goals and Non-Goals

### Goals
- **Data Collection**: Facilitate the consistent collection of data related to Chronic Wasting Disease (CWD) from multiple sources.
- **ETL & Durable Storage**: Consolidate CWD data into a unified, durable data structure with standardized schemas and validation.
- **Data Consumption**: Provide curated, purpose-built data views to support internal analytics, reporting, and public-facing use cases.

### Non-Goals
The following capabilities are explicitly out of scope for this system:
- **Real-Time Processing**: The system is not designed to support real-time or near-real-time data ingestion or processing. Data is processed on a periodic basis.
- **Public Data Submission**: Public-facing outputs are read-only. The system does not accept public submissions, comments, or corrections.
- **Advanced Analytics**: Advanced analytical processing (e.g., modeling, forecasting, or machine learning) is outside the current scope. Data is provided in curated but non-analytical form.
---

## 3. High-Level Architecture

### Overview Diagram


---

## 4. Major Components

### 4.1 Client / User Interface
Clients interact with the system through three primary access patterns, each optimized for a different audience and use case. These interaction modes are intentionally separated to support appropriate data governance, performance, and user experience requirements.

#### 4.1.1 Public Record Lookup (Self-Service Access)

Members of the public access individual records associated with harvested animals through a public-facing web interface. Users enter a unique access key into a search field, which triggers a client-side request to retrieve and display the corresponding record.

**Characteristics**
- Read-only access
- Record-level scope only (no bulk access)
- Key-based access controls
- Designed for ease of use and low technical literacy

**Constraints**
- No public data submission or modification
- Limited to pre-approved fields suitable for public release
- Optimized for lookup, not exploration or analysis

#### 4.1.2 Internal Analytics and Reporting (PowerBI)

Internal stakeholders primarily interact with the data through a Power BI dashboard that provides aggregated statistics, trends, and spatial visualizations related to Chronic Wasting Disease (CWD).

**Characteristics**
- Summary-level and aggregated views
- Spatial and temporal analysis
- Role-based access controls
- Read-optimized data views

**Typical Use Cases**
- Program oversight and reporting
- Trend monitoring
- Decision support

**Constraints**
- Not intended for record-level editing
- May contain sensitive information not suitable for broad distribution
- Data refresh frequency is aligned with scheduled ETL processes rather than real-time updates

#### 4.1.3 Internal Data Review and QA/QC (Spreadsheet Access)

Internal users also access a locally stored spreadsheet hosted on a secured network drive for limited manual data review and quality assurance activities.

**Characteristics**
- Used for spot checks and ad hoc QA/QC
- Access restricted to authorized internal users
- Supports manual review workflows not easily expressed in dashboards

**Constraints**
- Not a system of record
- Manual edits do not directly propagate back into upstream source systems
- Intended for temporary review rather than ongoing data management

---

### 4.2 Data Storage
**Responsibility**
- Persistence
- Versioning
- Auditability

**Key Technologies**
- Databases, file stores, schemas

**Notes**
- Backup strategy
- Retention policies

---

### 4.4 External Dependencies
**Responsibility**
- Third-party services or data sources

**Examples**
- Authentication providers
- External APIs
- Scheduled data feeds

**Risks**
- Availability
- Rate limits
- Data quality

---

## 5. Data Flow

### Typical Workflow

1. User performs <action>
2. Request flows through <component>
3. Data is validated and stored in <system>
4. Results are returned to <client>

![Data flow diagram](diagrams/data-flow.png)

---

## 6. Security and Trust Boundaries

### Authentication & Authorization
- How users or systems authenticate
- How permissions are enforced

### Trust Boundaries
- Where untrusted input enters the system
- Where data crosses network or system boundaries

---

## 7. Deployment Model

### Environments
- Development
