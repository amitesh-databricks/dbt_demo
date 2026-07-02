# Snowflake + dbt CI/CD POC

A Proof of Concept (POC) project demonstrating how to implement **CI/CD pipelines for dbt on Snowflake** using GitHub Actions.
This repository showcases modern analytics engineering practices including automated testing, deployment workflows, and environment-based execution for scalable data transformation projects.

---

# 🚀 Project Overview

This project demonstrates:

* Setting up a **dbt project** with Snowflake
* Implementing **CI/CD pipelines** using GitHub Actions
* Running automated:

  * dbt build
  * dbt test
  * dbt compile
  * dbt docs generation
* Environment separation:

  * DEV
  * QA/UAT
  * PROD
* Secure secret management using GitHub Secrets
* Best practices for analytics engineering workflows

---

# 🏗️ Tech Stack

| Tool           | Purpose                       |
| -------------- | ----------------------------- |
| Snowflake      | Cloud Data Warehouse          |
| dbt Core       | Data Transformation Framework |
| GitHub Actions | CI/CD Automation              |
| SQL            | Transformation Logic          |
| YAML           | Workflow Configuration        |

---

# 📂 Project Structure

```bash
.
├── models/                 # dbt models
├── macros/                 # reusable dbt macros
├── seeds/                  # static CSV seed files
├── snapshots/              # snapshot definitions
├── tests/                  # custom dbt tests
├── analyses/               # ad-hoc analysis SQL
├── .github/workflows/      # GitHub Actions CI/CD pipelines
├── dbt_project.yml         # dbt project configuration
├── profiles.yml            # local dbt profile (excluded from git)
└── README.md
```

---

# ⚙️ CI/CD Workflow

The CI/CD pipeline performs the following steps automatically:

## Pull Request Workflow

When a PR is created:

* Install dbt dependencies
* Validate SQL models
* Run:

  ```bash
  dbt compile
  dbt test
  ```
* Perform code quality checks

---

## Merge to Main Branch

On merge to `main`:

* Execute production deployment
* Run:

  ```bash
  dbt build
  ```
* Generate dbt documentation
* Publish artifacts

---

# 🔐 GitHub Secrets Required

Configure the following secrets in GitHub:

| Secret Name         | Description                  |
| ------------------- | ---------------------------- |
| SNOWFLAKE_ACCOUNT   | Snowflake account identifier |
| SNOWFLAKE_USER      | Snowflake username           |
| SNOWFLAKE_PASSWORD  | Snowflake password           |
| SNOWFLAKE_ROLE      | Snowflake role               |
| SNOWFLAKE_DATABASE  | Database name                |
| SNOWFLAKE_WAREHOUSE | Warehouse name               |
| SNOWFLAKE_SCHEMA    | Schema name                  |

---

# 🧪 Local Development Setup

## 1️⃣ Clone Repository

```bash
git clone https://github.com/amitesh-databricks/dbt_demo.git

cd dbt_demo
```

---

## 2️⃣ Create Virtual Environment

```bash
python -m venv venv

source venv/bin/activate
```

For Windows:

```bash
venv\Scripts\activate
```

---

## 3️⃣ Install dbt Snowflake Adapter

```bash
pip install dbt-snowflake
```

---

## 4️⃣ Configure profiles.yml

Create:

```bash
~/.dbt/profiles.yml
```

Example:

```yaml
dbt_demo:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <account>
      user: <username>
      password: <password>
      role: <role>
      database: <database>
      warehouse: <warehouse>
      schema: <schema>
      threads: 4
```

---

# ▶️ Running dbt Commands

## Validate Connection

```bash
dbt debug
```

## Install Dependencies

```bash
dbt deps
```

## Run Models

```bash
dbt run
```

## Run Tests

```bash
dbt test
```

## Build Entire Pipeline

```bash
dbt build
```

## Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

---

# 📊 Key Features Demonstrated

* Modular SQL transformations
* Reusable macros
* Automated CI/CD pipelines
* Environment-based deployment
* Data quality testing
* Documentation generation
* Analytics engineering best practices

---

# 🌟 Future Enhancements

* Add Slim CI
* Add dbt artifacts comparison
* Integrate with Terraform
* Add production deployment approvals
* Add Slack/Teams notifications
* Add dbt exposures and metrics
* Implement monitoring & observability

---

# 📘 Learning Outcomes

This POC helps understand:

* dbt project structure
* Snowflake integration with dbt
* GitHub Actions automation
* Modern DataOps practices
* CI/CD implementation for analytics workflows

---

# 🤝 Contributing

Contributions, suggestions, and improvements are welcome.

1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Create a Pull Request

---

# 📄 License

This project is for learning and demonstration purposes.

---

# 👨‍💻 Author

Created by Amitesh

GitHub Repository:
[dbt_demo Repository](https://github.com/amitesh-databricks/dbt_demo)

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### gold lineage
--<img width="1036" height="307" alt="image" src="https://github.com/user-attachments/assets/e30237c1-e91e-4ca2-94e4-419c7e0d6932" />

### Silver lineage
--<img width="1118" height="616" alt="image" src="https://github.com/user-attachments/assets/c0fbd4c4-8581-4ad4-a2d8-e0e2e3fe4921" />

### Bronze lineage
--- _stg_customers
--<img width="1291" height="396" alt="image" src="https://github.com/user-attachments/assets/796692cc-10b9-423b-ad43-e56739655015" />
-- _stg_orders
--<img width="1228" height="436" alt="image" src="https://github.com/user-attachments/assets/c416e03e-3cfc-4666-b6bb-ffdbf01192fb" />
--  _stg_payments
--<img width="1120" height="318" alt="image" src="https://github.com/user-attachments/assets/785fe86f-fd9c-4d54-bb96-b253c15de518" />
-- _stg_products
--<img width="973" height="256" alt="image" src="https://github.com/user-attachments/assets/d0b75030-7b63-405d-bb83-ecfe1a3cf9c4" />





