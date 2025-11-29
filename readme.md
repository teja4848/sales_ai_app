# 📊 AI-Powered Sales Query Assistant

*Part of EAS 503 – Working With Messy Data & Adding AI Insights*

## 📘 Overview

This project takes a **messy sales dataset**, cleans it, normalizes it into proper tables, uploads it to **PostgreSQL**, and then uses **AI** to help users ask SQL questions and get insights easily.

The goal is simple:
✨ *Turn messy data into clean tables and smart insights.*

You get:

* A clean normalized database
* A Streamlit app with password login
* AI that writes and explains SQL queries
* A notebook to run all analytical queries (ex1–ex11)

---

## ✨ Features

### 🔐 Login System

* Secure password protection (bcrypt hashed)
* Only allowed users can access the app

### 🧠 AI SQL Assistant

* Ask any natural-language question
* AI generates SQL for you
* AI explains results in simple terms

### 📊 Sales Insights

* Region revenue
* Top customers
* Best-selling products
* Quarterly & monthly trends
* Largest gaps between customer orders
* (From ex1–ex11 queries)

### 🗄️ Database

* Fully normalized PostgreSQL schema
* Auto population script included
* All tables created exactly as required for EAS 503

### 💻 Streamlit App

Two-column layout:

* Left: Ask questions
* Right: View SQL + results
* Smooth UI for demoing your project

---

## ⚙️ Tech Stack

**Backend:** Python, PostgreSQL
**Frontend:** Streamlit
**AI Engine:** OpenAI API
**ETL & Cleaning:** Jupyter Notebook
**Auth:** bcrypt hashed password
**Deployment:** Render (DB), Streamlit Cloud / Local

---

# 🚀 How to Run the Project

## 1️⃣ Clone the Repo

```bash
git clone <your_repo_link>
cd <repo_name>
```

## 2️⃣ Configure Environment Variables

Rename:

```text
sample.env → .env
```

Create this file at:
`.streamlit/secrets.toml`

```toml
[sales_db]
POSTGRES_USERNAME = ""
POSTGRES_PASSWORD = ""
POSTGRES_SERVER   = ""
POSTGRES_DATABASE = ""

OPENAI_API_KEY    = ""
HASHED_PASSWORD   = ""
```

## 3️⃣ Create Virtual Environment

```bash
python -m venv .venv
```

### Windows

```bash
.\.venv\Scripts\activate
```

### macOS/Linux

```bash
source .venv/bin/activate
```

## 4️⃣ Install Packages

```bash
pip install -r requirements.txt
```

## 5️⃣ Generate Login Password

```bash
python generate_password.py
```

Copy the hash → paste into `HASHED_PASSWORD` in `secrets.toml`.

## 6️⃣ Test Database Connection

```bash
python test_render_database.py
```

## 7️⃣ Populate the Database

```bash
python populate_db.py
```

## 8️⃣ Launch Streamlit App

```bash
streamlit run streamlit_app.py
```

---

# 🔐 Manual Password Hashing (Optional)

```python
import bcrypt
password = "your_password".encode('utf-8')
print(bcrypt.hashpw(password, bcrypt.gensalt()).decode())
```

---

# 📘 Included Files

```text
streamlit_app.py          → Main AI + UI app
populate_db.py            → Loads all normalized tables
test_render_database.py   → Tests connection to Render DB
miniproject2.ipynb        → All ex1–ex11 PostgreSQL queries
generate_password.py      → Creates bcrypt password hash
requirements.txt
.env / .streamlit/secrets.toml
```

---

# ✅ EAS 503 Live Demo Checklist

* ✔️ Render PostgreSQL running
* ✔️ Normalized tables created
* ✔️ Jupyter Notebook connected
* ✔️ ex1–ex11 converted to Postgres
* ✔️ GitHub repo ready
* ✔️ Streamlit app with password login
* ✔️ AI SQL generator working
* ✔️ Clean, professional project for demo

🌐 Live Demo  
See the AI-Powered Sales Query Assistant in action!

👉 Try the live Streamlit app here: **[Live Demo Link]**  

---

# 📩 Feedback

If you have new ideas or want to improve the app, feel free to connect anytime!
