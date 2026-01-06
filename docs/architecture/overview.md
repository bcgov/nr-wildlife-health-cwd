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
**Responsibility**
- What this component does

**Key Technologies**
- Frameworks, libraries, protocols

**Notes**
- Assumptions or constraints

---

### 4.2 API / Application Layer
**Responsibility**
- Business logic
- Validation
- Authorization

**Key Technologies**
- Language, framework, hosting model

**Notes**
- Stateless vs stateful
- Scaling considerations

---

### 4.3 Data Storage
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
