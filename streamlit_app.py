"""
Sales AI SQL Assistant (Optimized with SQLAlchemy Connection)

Notes:
- Uses SQLAlchemy with secrets under [sales_db].
- Keeps original UI/CSS & layout/style intact.
- Adds caching for dashboard & preview queries to avoid repeated network calls.
- Adds explicit "Load Dashboard" action inside the expander to lazy-load heavy queries.
"""

import re
import bcrypt
import pandas as pd
import streamlit as st
from openai import OpenAI
from typing import Optional
from sqlalchemy import create_engine, text  # <-- added

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Sales SQL Assistant",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Secrets loading
# ---------------------------------------------------------------------------
config = st.secrets["sales_db"]
OPENAI_API_KEY = config["OPENAI_API_KEY"]
HASHED_PASSWORD = config["HASHED_PASSWORD"].encode("utf-8")

# ---------------------------------------------------------------------------
# Database schema description (used as context for GPT)
# ---------------------------------------------------------------------------
DATABASE_SCHEMA = """
Normalized Sales Database (Schema Overview)

region (
    regionid SERIAL PRIMARY KEY,
    region TEXT NOT NULL
)

country (
    countryid SERIAL PRIMARY KEY,
    country TEXT NOT NULL,
    regionid INTEGER NOT NULL REFERENCES region(regionid)
)

customer (
    customerid SERIAL PRIMARY KEY,
    firstname TEXT NOT NULL,
    lastname TEXT NOT NULL,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    countryid INTEGER REFERENCES country(countryid)
)

productcategory (
    productcategoryid SERIAL PRIMARY KEY,
    productcategory TEXT,
    productcategorydescription TEXT
)

product (
    productid SERIAL PRIMARY KEY,
    productname TEXT,
    productunitprice REAL,
    productcategoryid INTEGER REFERENCES productcategory(productcategoryid)
)

orderdetail (
    orderid SERIAL PRIMARY KEY,
    customerid INTEGER REFERENCES customer(customerid),
    productid INTEGER REFERENCES product(productid),
    orderdate DATE,
    quantityordered INTEGER
)

Notes:
- Revenue = product.productunitprice * orderdetail.quantityordered
- ORDERDATE is a DATE column (PostgreSQL DATE)
- Tables connect: Region → Country → Customer → Orders → Products
"""

# ---------------------------------------------------------------------------
# Authentication UI (keeps your style)
# ---------------------------------------------------------------------------
def login_screen():
    """Modern login with horizontal scrolling features in header & security notice styled."""

    # --- CSS for styling ---
    st.markdown(
        """
        <style>
            /* LOGIN BUTTON CENTERED */
            .stButton>button {
                background: linear-gradient(90deg, #6A5ACD, #7B68EE);
                color: white;
                border: none;
                padding: 10px 22px;
                font-size: 16px;
                border-radius: 8px;
                font-weight: 600;
                width: 80%;
                margin-left: 170px;
                margin-top: 12px;
                transition: 0.2s;
            }
            .stButton>button:hover {
                background: linear-gradient(90deg, #5D50C7, #6A5ACD);
                transform: scale(1.02);
            }

            /* TALL HEADER WITH CURVED EDGES */
            .top-header {
                background: #6A5ACD;
                padding: 50px 0;
                text-align: center;
                color: white;
                font-size: 42px;
                font-weight: 900;
                letter-spacing: 1.5px;
                position: relative;
                border-radius: 30px 30px 0 0;
            }

            /* FEATURES SCROLL */
            .features-scroll {
                display: flex;
                overflow-x: auto;
                margin-top: 15px;
                padding: 8px 0;
                gap: 30px;
                justify-content: center;
                font-size: 20px;
                font-weight: bold;
            }

            .features-scroll::-webkit-scrollbar {
                height: 6px;
            }
            .features-scroll::-webkit-scrollbar-thumb {
                background: #7B68EE;
                border-radius: 3px;
            }

            /* SECURITY NOTICE BOX */
            .security-box {
                background: #f3f0ff;
                border-left: 5px solid #7B68EE;
                padding: 15px;
                border-radius: 0 0 30px 30px;
                margin-top: 30px;
                font-size: 35px;
                color: #333;
            }

            /* Subheading for login */
            .sub-title {
                text-align: center;
                font-size: 30px;
                font-weight: 600;
                color: black;
                margin-top: 20px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ---------- HEADER ----------
    st.markdown(
        """
        <div class="top-header">
            🚀 Sales AI Bot
            <div class="features-scroll">
                <span>✨ Natural-language → SQL</span>
                <span>📊 Smart KPIs & Charts</span>
                <span>🧠 AI insights & explanations</span>
                <span>🔒 Secure bcrypt login</span>
                <span>📈 Live dashboards</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------- CENTERED LOGIN ----------
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        st.markdown(
            "<h3 style='text-align:center; color:black; font-weight:800; font-size:28px;'>🔒 Secure Login</h3>",
            unsafe_allow_html=True
        )
        password = st.text_input(
            "Password",
            type="password",
            placeholder="Enter your secure password",
        )
        login_clicked = st.button("Login")

    # ---------- LOGIN LOGIC ----------
    if login_clicked:
        if not password:
            st.warning("⚠️ Please enter the password.")
            return

        try:
            if bcrypt.checkpw(password.encode("utf-8"), HASHED_PASSWORD):
                st.session_state.logged_in = True
                st.success("Login successful! Redirecting…")
                st.rerun()
            else:
                st.error("❌ Incorrect password.")
        except Exception as e:
            st.error(f"Authentication error: {e}")

    # ---------- SECURITY NOTICE ----------
    st.markdown(
        """
        <div class="security-box">
            <b>🔐 Security Notice</b><br>
            • Passwords are protected using bcrypt hashing<br>
            • Your session is secure and isolated<br>
            • You will remain logged in until logout
        </div>
        """,
        unsafe_allow_html=True,
    )


def ensure_logged_in():
    if not st.session_state.get("logged_in"):
        login_screen()
        st.stop()

# ---------------------------------------------------------------------------
# Database Utilities (SQLAlchemy + caching)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_engine():
    """
    Return a SQLAlchemy engine using the [sales_db] block from secrets.toml.
    """
    cfg = st.secrets["sales_db"]

    user = cfg["POSTGRES_USERNAME"]
    password = cfg["POSTGRES_PASSWORD"]
    host = cfg["POSTGRES_SERVER"]
    database = cfg["POSTGRES_DATABASE"]
    port = cfg.get("POSTGRES_PORT", "5432")  # optional

    conn_str = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    return create_engine(conn_str)


@st.cache_data(ttl=300)
def run_query(sql: str, ttl: int = 300) -> Optional[pd.DataFrame]:
    """
    Run a SQL query using SQLAlchemy and cache the result.
    ttl controls caching; by default dashboard/preview use 300s caching.
    """
    engine = get_engine()

    sql = _strip_sql_comments(sql).strip()
    if not sql:
        st.error("No SQL to execute.")
        return None

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return None


@st.cache_data(ttl=300)
def run_scalar(sql: str, ttl: int = 300):
    """Execute a scalar query and return first value (cached)."""
    df = run_query(sql, ttl=ttl)
    if df is not None and not df.empty:
        return df.iloc[0, 0]
    return None


def _strip_sql_comments(sql: str) -> str:
    """Remove lines starting with -- comments."""
    lines = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# OpenAI Utilities (cached client, trimmed tokens to improve latency)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_openai_client():
    return OpenAI(api_key=OPENAI_API_KEY)


def extract_sql_from_response(text: str) -> str:
    """
    Extract the first SQL statement (SELECT / WITH) from an explanation+SQL response.

    Priority:
    1. SQL inside ```sql ... ``` code fences.
    2. Otherwise, find first SELECT/WITH and cut until the first semicolon.
    """
    # 1) Try ```sql ... ``` block
    fence_match = re.search(r"```sql\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fence_match:
        sql_block = fence_match.group(1).strip()
        if ";" in sql_block:
            sql_block = sql_block.split(";", 1)[0]
        return sql_block.strip()

    # 2) Fallback: find first SELECT or WITH
    cleaned = text.strip()
    match = re.search(r"\b(select|with)\b", cleaned, flags=re.IGNORECASE)
    if match:
        cleaned = cleaned[match.start():]

    if ";" in cleaned:
        cleaned = cleaned.split(";", 1)[0]

    return cleaned.strip()


def generate_sql_and_explanation(question: str):
    """
    Ask GPT for explanation + SQL, then extract SQL.
    Uses smaller max_tokens and lower temperature for faster responses.
    """
    client = get_openai_client()
    prompt = f"""
You are an expert PostgreSQL assistant.

Given the following schema and a user's question, respond in TWO PARTS:

1. A short natural-language explanation of what you are going to query.
2. A single SQL SELECT query wrapped in a ```sql ... ``` block.

SCHEMA:
{DATABASE_SCHEMA}

User Question:
{question}

Guidelines for the SQL:
- Use joins to show readable fields when relevant (region, country, productname, firstname/lastname).
- Use SUM/COUNT/AVG where appropriate.
- Use LIMIT 100 for queries that may produce many rows.
- Revenue = product.productunitprice * orderdetail.quantityordered.
- Only ONE SELECT statement (it may use WITH/CTEs).
""".strip()

    try:
        # Reduced max tokens for speed while still keeping capability
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.15,
            max_tokens=420,
            messages=[{"role": "user", "content": prompt}],
        )

        full_text = response.choices[0].message.content
        sql = extract_sql_from_response(full_text)
        return full_text, sql
    except Exception as e:
        st.error(f"OpenAI error while generating SQL: {e}")
        return None, None


def explain_results(question: str, sql: str, df: pd.DataFrame):
    """
    Ask GPT to explain the result table in simple bullet points.
    Use a small sample of rows to reduce token consumption.
    """
    client = get_openai_client()
    sample_records = df.head(6).to_dict(orient="records")
    prompt = f"""
You are a data analyst explaining query results to a non-technical stakeholder.

User question:
{question}

SQL that was run:
{sql}

Sample of the results (first rows):
{sample_records}

Explain in 3–6 concise bullet points:
- What this table is showing
- Any obvious patterns or rankings
- How someone could use this insight for decision-making

No raw SQL, no code fences. Just bullet points.
""".strip()

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.35,
            max_tokens=220,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.choices[0].message.content
    except Exception as e:
        st.error(f"Error generating explanation of results: {e}")
        return None

# ---------------------------------------------------------------------------
# Main App (keeps your UI & behavior; optimized internals)
# ---------------------------------------------------------------------------

def main():
    ensure_logged_in()

    # ---------- SIDEBAR ----------
    st.sidebar.title("📊 Sales SQL Assistant")
    st.sidebar.markdown(
        """
Use this tool to:

- Explore a **normalized sales database**
- Ask questions in **plain English**
- Let AI generate **PostgreSQL** queries
- Edit & run those queries safely
- View KPIs, charts, and **recent queries**
        """.strip()
    )

    st.sidebar.markdown("### 🔎 Example questions")
    st.sidebar.markdown(
        """
- How many customers are in the database?
- Which region has the highest total sales?
- Show the top 10 products by revenue.
- Rank all customers by total spending and show the top 20.
- For each region, rank countries by total revenue.
- Show monthly revenue over time.
- What percentage of total revenue does each region contribute?
        """.strip()
    )

    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 Logout"):
        st.session_state.logged_in = False
        st.rerun()
    st.sidebar.caption("Password-protected. Backed by bcrypt & PostgreSQL on Render.")

    # ---------- HEADER ----------
    st.title("📊 AI-Powered Sales SQL Assistant")
    st.write(
        "Ask questions about your sales data, let the AI generate SQL, "
        "and inspect both the raw results and a friendly explanation."
    )
    st.markdown("---")

    # ---------- DASHBOARD (LAZY-LOAD: user clicks to load dashboard) ----------
    with st.expander("📈 Quick Sales Overview (click to expand)", expanded=False):
        # Provide a button so the content doesn't run automatically on every rerun
        load_dash = st.button("Load Dashboard")
        if load_dash:
            # 4 quick metrics (cached and fast)
            m1, m2, m3, m4 = st.columns(4)

            with m1:
                total_customers = run_scalar("SELECT COUNT(*) FROM customer;", ttl=300)
                st.metric(
                    "Customers",
                    f"{int(total_customers):,}" if total_customers is not None else "–",
                )

            with m2:
                total_orders = run_scalar("SELECT COUNT(*) FROM orderdetail;", ttl=300)
                st.metric(
                    "Order Rows",
                    f"{int(total_orders):,}" if total_orders is not None else "–",
                )

            with m3:
                total_revenue = run_scalar(
                    """
                    SELECT SUM(p.productunitprice * od.quantityordered)
                    FROM orderdetail od
                    JOIN product p ON od.productid = p.productid;
                    """,
                    ttl=300
                )
                st.metric(
                    "Total Revenue",
                    f"${float(total_revenue):,.2f}" if total_revenue is not None else "–",
                )

            with m4:
                top_region_df = run_query(
                    """
                    SELECT r.region, SUM(p.productunitprice * od.quantityordered) AS total_sales
                    FROM orderdetail od
                    JOIN customer c ON od.customerid = c.customerid
                    JOIN country co ON c.countryid = co.countryid
                    JOIN region r ON co.regionid = r.regionid
                    JOIN product p ON od.productid = p.productid
                    GROUP BY r.region
                    ORDER BY total_sales DESC
                    LIMIT 1;
                    """,
                    ttl=300
                )
                if top_region_df is not None and not top_region_df.empty:
                    st.metric("Top Region", top_region_df.loc[0, "region"])
                else:
                    st.metric("Top Region", "–")

            c1, c2 = st.columns(2)

            with c1:
                monthly_df = run_query(
                    """
                    SELECT DATE_TRUNC('month', od.orderdate) AS month,
                           SUM(p.productunitprice * od.quantityordered) AS revenue
                    FROM orderdetail od
                    JOIN product p ON od.productid = p.productid
                    GROUP BY month
                    ORDER BY month;
                    """,
                    ttl=300
                )
                if monthly_df is not None and not monthly_df.empty:
                    monthly_df = monthly_df.set_index("month")
                    st.line_chart(monthly_df["revenue"])
                else:
                    st.caption("No monthly revenue data available.")

            with c2:
                top_prod_df = run_query(
                    """
                    SELECT p.productname, SUM(p.productunitprice * od.quantityordered) AS total_revenue
                    FROM orderdetail od
                    JOIN product p ON od.productid = p.productid
                    GROUP BY p.productname
                    ORDER BY total_revenue DESC
                    LIMIT 10;
                    """,
                    ttl=300
                )
                if top_prod_df is not None and not top_prod_df.empty:
                    st.bar_chart(top_prod_df.set_index("productname")["total_revenue"])
                else:
                    st.caption("No product revenue data available.")

    st.markdown("---")

    # ---------- MAIN WORK AREA ----------
    st.session_state.setdefault("history", [])
    st.session_state.setdefault("generated_sql", "")
    st.session_state.setdefault("full_ai_response", "")

    left, right = st.columns([2.2, 1])

    # ---------------- LEFT SIDE: AI Q&A ----------------
    with left:
        st.subheader("Ask a question")
        question = st.text_area(
            "Your question:",
            height=100,
            placeholder="e.g. Rank all customers by total spending and show the top 20.",
        )

        generate = st.button("✨ Generate SQL")
        if generate:
            cleaned_q = (question or "").strip()
            if len(cleaned_q) < 10:
                st.warning(
                    "Please ask a more descriptive question (at least a full sentence) "
                    "so the AI can generate a useful query."
                )
            else:
                with st.spinner("Talking to the AI..."):
                    full_text, sql = generate_sql_and_explanation(cleaned_q)
                if sql:
                    st.session_state.generated_sql = sql
                    st.session_state.full_ai_response = full_text
                else:
                    st.error("Could not extract a valid SQL query from the AI response.")

        if st.session_state.full_ai_response:
            st.markdown("#### 🧠 AI reasoning (what it plans to query)")
            st.write(st.session_state.full_ai_response)

        if st.session_state.generated_sql:
            st.subheader("Extracted SQL to run")
            edited_sql = st.text_area(
                "Review / modify before running:",
                value=st.session_state.generated_sql,
                height=200,
            )

            if st.button("▶️ Run Query"):
                # Run live (no cache) for user-edited query; but use a short caching window to avoid re-query flood
                with st.spinner("Executing query..."):
                    # We use ttl=0 to explicitly fetch live results; if you'd like a short cache, set ttl=5
                    df = run_query(edited_sql, ttl=0)

                if df is not None:
                    st.success(f"Returned {len(df)} rows")
                    st.dataframe(df, use_container_width=True)

                    with st.spinner("Explaining these results..."):
                        explanation = explain_results(question or "(no question provided)", edited_sql, df)
                    if explanation:
                        st.markdown("#### 📌 Summary of Results")
                        st.markdown(explanation)

                    # Keep a short history of queries
                    st.session_state.history.append({"q": question, "sql": edited_sql})

    # ---------------- RIGHT SIDE: Preview + History ----------------
    with right:
        st.subheader("Quick Table Preview")
        table = st.selectbox(
            "Choose table:",
            [
                "customer",
                "product",
                "orderdetail",
                "country",
                "region",
                "productcategory",
            ],
        )

        if st.button("Preview 10 rows"):
            # Use cached preview (fast). Keep the LIMIT in the SQL to reduce payload size.
            prev_df = run_query(f"SELECT * FROM {table} LIMIT 10;", ttl=300)
            if prev_df is not None:
                # To keep preview lightweight, show up to first 8 columns
                if prev_df.shape[1] > 8:
                    prev_df = prev_df.iloc[:, :8]
                st.dataframe(prev_df, use_container_width=True)

        if st.session_state.history:
            st.subheader("Recent Queries")
            for item in reversed(st.session_state.history[-6:]):
                with st.expander((item["q"] or "Saved query")[:60] + "…"):
                    st.code(item["sql"], language="sql")


if __name__ == "__main__":
    main()
