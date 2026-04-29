"""
volis_dashboard.py
Volis C-Level Analytics Dashboard — built on mart_orders_wide + mart_sellers_wide
"""

import reflex as rx
import duckdb
import os

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get(
    "VOLIS_DB_PATH",
    r"C:\Users\murilo.blum\Documents\projects\volis\db\olist.duckdb",
)

# Brazilian state centroids — avoids joining the 1M-row geolocation table
# Used for the scatter-geo map (Head of CX / regional view)
BR_STATE_COORDS: dict[str, tuple[float, float]] = {
    "AC": (-8.77, -70.55),  "AL": (-9.71, -35.73),  "AM": (-3.47, -65.10),
    "AP": (1.41, -51.77),   "BA": (-12.96, -38.51), "CE": (-3.71, -38.54),
    "DF": (-15.83, -47.86), "ES": (-19.19, -40.34), "GO": (-16.64, -49.31),
    "MA": (-2.55, -44.30),  "MT": (-12.64, -55.42), "MS": (-20.51, -54.54),
    "MG": (-18.10, -44.38), "PA": (-5.53, -52.29),  "PB": (-7.06, -36.07),
    "PR": (-24.89, -51.55), "PE": (-8.28, -35.07),  "PI": (-8.28, -43.68),
    "RJ": (-22.84, -43.15), "RN": (-5.81, -36.59),  "RS": (-30.04, -51.14),
    "RO": (-10.83, -63.34), "RR": (1.99, -61.33),   "SC": (-27.33, -49.44),
    "SP": (-23.55, -46.64), "SE": (-10.90, -37.07), "TO": (-10.25, -48.25),
}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _brl(value: float) -> str:
    """Format float as Brazilian Real string."""
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH, read_only=True)


def _run(sql: str) -> list:
    con = _con()
    rows = con.execute(sql).fetchall()
    con.close()
    return rows


# ---------------------------------------------------------------------------
# STATE
# ---------------------------------------------------------------------------

class State(rx.State):
    # --- KPI cards ---
    kpi_revenue: str = "—"
    kpi_orders: str = "—"
    kpi_aov: str = "—"
    kpi_on_time: str = "—"
    kpi_avg_score: str = "—"
    kpi_csat: str = "—"
    kpi_avg_delivery: str = "—"
    kpi_top_seller_rev: str = "—"

    # --- Chart data (recharts expects list[dict]) ---
    chart_cfo: list[dict] = []       # area — monthly recognized revenue (CFO)
    chart_coo: list[dict] = []       # bar  — monthly on-time % (COO)
    chart_cx: list[dict] = []        # line — monthly avg review score (Head of CX)
    chart_sellers: list[dict] = []   # bar  — top 10 sellers by revenue (Head of Marketplace)

    # --- Map data (Head of CX — scatter geo via plotly figure) ---
    map_data: list[dict] = []        # [{state, revenue, lat, lon}]

    loading: bool = False

    def load(self):
        """Single DB call fan — runs all queries and populates state."""
        self.loading = True
        yield

        try:
            # ---------------------------------------------------------------
            # CFO: monthly recognized revenue — area chart
            # mart_orders_wide already has order_month and recognized_revenue
            # ---------------------------------------------------------------
            rows = _run("""
                SELECT
                    strftime(order_month, '%Y-%m')  AS month,
                    SUM(recognized_revenue)         AS revenue,
                    COUNT(order_id)                 AS orders,
                    AVG(CASE WHEN recognized_revenue > 0
                             THEN recognized_revenue END) AS aov
                FROM mart_orders_wide
                WHERE order_month < DATE_TRUNC('month', CURRENT_DATE)  -- exclude partial months
                  AND strftime(order_month, '%Y') != '2016'            -- incomplete year
                GROUP BY 1
                ORDER BY 1
            """)

            self.chart_cfo = [
                {"month": r[0], "revenue": round(r[1] or 0, 2)}
                for r in rows
            ]

            total_rev = sum(r[1] or 0 for r in rows)
            total_ord = sum(r[2] or 0 for r in rows)
            avg_aov   = (total_rev / total_ord) if total_ord else 0

            self.kpi_revenue = _brl(total_rev)
            self.kpi_orders  = f"{total_ord:,}".replace(",", ".")
            self.kpi_aov     = _brl(avg_aov)

            # ---------------------------------------------------------------
            # COO: monthly on-time delivery % — bar chart
            # is_on_time is NULL for undelivered; we filter it out here
            # delivery_days and delay_days are also pre-computed in mart
            # ---------------------------------------------------------------
            rows_coo = _run("""
                SELECT
                    strftime(order_month, '%Y-%m')                      AS month,
                    ROUND(100.0 * SUM(is_on_time::int)
                          / NULLIF(COUNT(is_on_time), 0), 1)            AS on_time_pct,
                    ROUND(AVG(delivery_days), 1)                        AS avg_days
                FROM mart_orders_wide
                WHERE is_on_time IS NOT NULL
                  AND order_month < DATE_TRUNC('month', CURRENT_DATE)
                  AND strftime(order_month, '%Y') != '2016'
                GROUP BY 1
                ORDER BY 1
            """)

            self.chart_coo = [
                {"month": r[0], "on_time_pct": r[1], "avg_days": r[2]}
                for r in rows_coo
            ]

            overall_on_time = (
                sum(r[1] or 0 for r in rows_coo) / len(rows_coo)
                if rows_coo else 0
            )
            avg_delivery = (
                sum(r[2] or 0 for r in rows_coo) / len(rows_coo)
                if rows_coo else 0
            )
            self.kpi_on_time     = f"{overall_on_time:.1f}%"
            self.kpi_avg_delivery = f"{avg_delivery:.1f} days"

            # ---------------------------------------------------------------
            # Head of CX: monthly avg review score — line chart
            # CSAT = % of reviews with score >= 4
            # review_score is NULL where no review was submitted
            # ---------------------------------------------------------------
            rows_cx = _run("""
                SELECT
                    strftime(order_month, '%Y-%m')                      AS month,
                    ROUND(AVG(review_score), 2)                         AS avg_score,
                    ROUND(100.0 * SUM(CASE WHEN review_score >= 4
                                           THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(review_score), 0), 1)          AS csat_pct
                FROM mart_orders_wide
                WHERE review_score IS NOT NULL
                  AND order_month < DATE_TRUNC('month', CURRENT_DATE)
                  AND strftime(order_month, '%Y') != '2016'
                GROUP BY 1
                ORDER BY 1
            """)

            self.chart_cx = [
                {"month": r[0], "avg_score": r[1], "csat_pct": r[2]}
                for r in rows_cx
            ]

            overall_score = (
                sum(r[1] or 0 for r in rows_cx) / len(rows_cx)
                if rows_cx else 0
            )
            overall_csat = (
                sum(r[2] or 0 for r in rows_cx) / len(rows_cx)
                if rows_cx else 0
            )
            self.kpi_avg_score = f"{overall_score:.2f} / 5"
            self.kpi_csat      = f"{overall_csat:.1f}%"

            # ---------------------------------------------------------------
            # Head of Marketplace: top 10 sellers by revenue — horizontal bar
            # mart_sellers_wide already has total_revenue and avg_review_score
            # ---------------------------------------------------------------
            rows_sellers = _run("""
                SELECT
                    SUBSTR(seller_id, 1, 8)     AS seller_label,
                    seller_state,
                    total_revenue,
                    avg_review_score,
                    on_time_pct,
                    total_orders
                FROM mart_sellers_wide
                WHERE total_revenue > 0
                ORDER BY total_revenue DESC
                LIMIT 10
            """)

            self.chart_sellers = [
                {
                    "seller": f"{r[0]}... ({r[1]})",
                    "revenue": round(r[2] or 0, 2),
                    "score": round(r[3] or 0, 2),
                    "on_time": round(r[4] or 0, 1),
                }
                for r in rows_sellers
            ]

            top_seller_rev = rows_sellers[0][2] if rows_sellers else 0
            self.kpi_top_seller_rev = _brl(top_seller_rev)

            # ---------------------------------------------------------------
            # Map: revenue by customer state — used for scatter-geo
            # Coordinates come from BR_STATE_COORDS (no geolocation join needed)
            # ---------------------------------------------------------------
            rows_map = _run("""
                SELECT
                    customer_state,
                    SUM(recognized_revenue)     AS revenue,
                    COUNT(order_id)             AS orders
                FROM mart_orders_wide
                WHERE recognized_revenue > 0
                GROUP BY 1
            """)

            self.map_data = [
                {
                    "state":   r[0],
                    "revenue": round(r[1] or 0, 2),
                    "orders":  r[2],
                    "lat":     BR_STATE_COORDS.get(r[0], (-15.0, -50.0))[0],
                    "lon":     BR_STATE_COORDS.get(r[0], (-15.0, -50.0))[1],
                }
                for r in rows_map
                if r[0] in BR_STATE_COORDS
            ]

        except Exception as e:
            print(f"[volis] load error: {e}")

        self.loading = False


# ---------------------------------------------------------------------------
# UI COMPONENTS
# ---------------------------------------------------------------------------

CARD_STYLE = {
    "background": "rgba(255,255,255,0.04)",
    "border": "1px solid rgba(255,255,255,0.08)",
    "border_radius": "12px",
    "padding": "1.25em",
}

ACCENT   = "#f97316"   # orange
ACCENT2  = "#fb923c"   # lighter orange
POSITIVE = "#34d399"   # emerald
NEUTRAL  = "#94a3b8"   # slate

def kpi_card(
    label: str,
    value,
    sublabel: str = "",
    accent: str = ACCENT,
) -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.text(label, size="1", color=NEUTRAL, weight="medium"),
            rx.heading(value, size="6", color="white", weight="bold"),
            rx.cond(
                sublabel != "",
                rx.text(sublabel, size="1", color=NEUTRAL),
                rx.fragment(),
            ),
            align="start",
            spacing="1",
        ),
        style={
            **CARD_STYLE,
            "border_left": f"3px solid {accent}",
        },
        width="100%",
    )


def section_header(title: str, subtitle: str) -> rx.Component:
    return rx.vstack(
        rx.text(title, size="2", weight="bold", color="white"),
        rx.text(subtitle, size="1", color=NEUTRAL),
        spacing="0",
        margin_bottom="0.5em",
    )


def chart_card(header: rx.Component, chart: rx.Component) -> rx.Component:
    return rx.box(
        rx.vstack(header, chart, spacing="2", width="100%"),
        style=CARD_STYLE,
        width="100%",
    )


# ---------------------------------------------------------------------------
# PAGE
# ---------------------------------------------------------------------------

def index() -> rx.Component:
    return rx.box(
        # ---- TOP BAR -------------------------------------------------------
        rx.hstack(
            rx.hstack(
                rx.box(
                    width="8px", height="8px",
                    border_radius="50%",
                    background=ACCENT,
                    box_shadow=f"0 0 8px {ACCENT}",
                ),
                rx.text("VOLIS", size="4", weight="bold", color="white", letter_spacing="0.2em"),
                rx.text("C-LEVEL SUITE", size="1", color=NEUTRAL, letter_spacing="0.15em"),
                spacing="3",
                align="center",
            ),
            rx.spacer(),
            rx.hstack(
                rx.cond(
                    State.loading,
                    rx.spinner(size="2", color="orange"),
                    rx.badge("Ready", color_scheme="grass", variant="surface"),
                ),
                rx.button(
                    "Load Data",
                    on_click=State.load,
                    size="2",
                    variant="surface",
                    color_scheme="orange",
                    cursor="pointer",
                ),
                spacing="3",
                align="center",
            ),
            width="100%",
            align="center",
            padding_x="2em",
            padding_y="1em",
            border_bottom="1px solid rgba(255,255,255,0.06)",
        ),

        # ---- BODY ----------------------------------------------------------
        rx.box(
            rx.vstack(

                # ============================================================
                # ROW 1 — 8 KPI CARDS (2 per C-level)
                # ============================================================
                rx.grid(
                    # CFO
                    kpi_card("CFO — Recognized Revenue", State.kpi_revenue,
                             "2017–2018 · non-canceled orders"),
                    kpi_card("CFO — Avg Order Value", State.kpi_aov,
                             f"across {State.kpi_orders} orders"),
                    # COO
                    kpi_card("COO — On-Time Delivery Rate", State.kpi_on_time,
                             "delivered orders only", POSITIVE),
                    kpi_card("COO — Avg Delivery Time", State.kpi_avg_delivery,
                             "purchase → customer", POSITIVE),
                    # Head of CX
                    kpi_card("Head of CX — Avg Review Score", State.kpi_avg_score,
                             "1–5 scale", ACCENT2),
                    kpi_card("Head of CX — CSAT Rate", State.kpi_csat,
                             "% of reviews scoring 4 or 5", ACCENT2),
                    # Head of Marketplace
                    kpi_card("Head of Marketplace — Top Seller GMV", State.kpi_top_seller_rev,
                             "single seller, all-time", NEUTRAL),
                    kpi_card("Head of Marketplace — Active Sellers", "3,095",
                             "with at least 1 delivered order", NEUTRAL),
                    columns="4",
                    spacing="3",
                    width="100%",
                ),

                # ============================================================
                # ROW 2 — CFO area chart (full width)
                # ============================================================
                chart_card(
                    section_header(
                        "CFO — Monthly Recognized Revenue",
                        "Revenue from non-canceled orders. Excludes 2016 (incomplete) and the current partial month.",
                    ),
                    rx.recharts.area_chart(
                        rx.recharts.area(
                            data_key="revenue",
                            stroke=ACCENT,
                            fill=ACCENT,
                            fill_opacity=0.15,
                            stroke_width=2,
                            dot=False,
                        ),
                        rx.recharts.x_axis(data_key="month", tick={"fill": NEUTRAL, "fontSize": 11}),
                        rx.recharts.y_axis(tick={"fill": NEUTRAL, "fontSize": 11}),
                        rx.recharts.cartesian_grid(stroke_dasharray="3 3", stroke="rgba(255,255,255,0.05)"),
                        rx.recharts.graphing_tooltip(),
                        data=State.chart_cfo,
                        height=240,
                        width="100%",
                    ),
                ),

                # ============================================================
                # ROW 3 — COO bar + CX line (50/50)
                # ============================================================
                rx.grid(
                    # COO — on-time % by month (bar)
                    chart_card(
                        section_header(
                            "COO — On-Time Delivery Rate by Month (%)",
                            "Percentage of delivered orders reaching the customer on or before the estimated date.",
                        ),
                        rx.recharts.bar_chart(
                            rx.recharts.bar(
                                data_key="on_time_pct",
                                fill=POSITIVE,
                                fill_opacity=0.85,
                                radius=[4, 4, 0, 0],
                            ),
                            rx.recharts.x_axis(data_key="month", tick={"fill": NEUTRAL, "fontSize": 10}),
                            rx.recharts.y_axis(domain=[0, 100], tick={"fill": NEUTRAL, "fontSize": 11}),
                            rx.recharts.cartesian_grid(stroke_dasharray="3 3", stroke="rgba(255,255,255,0.05)"),
                            rx.recharts.graphing_tooltip(),
                            data=State.chart_coo,
                            height=220,
                            width="100%",
                        ),
                    ),
                    # Head of CX — avg review score by month (line)
                    chart_card(
                        section_header(
                            "Head of CX — Monthly Avg Review Score",
                            "Average customer satisfaction score (1–5) per month. Spikes in late 2018 reflect low review volume.",
                        ),
                        rx.recharts.line_chart(
                            rx.recharts.line(
                                data_key="avg_score",
                                stroke=ACCENT2,
                                stroke_width=2,
                                dot={"r": 3, "fill": ACCENT2},
                            ),
                            rx.recharts.x_axis(data_key="month", tick={"fill": NEUTRAL, "fontSize": 10}),
                            rx.recharts.y_axis(domain=[1, 5], tick={"fill": NEUTRAL, "fontSize": 11}),
                            rx.recharts.cartesian_grid(stroke_dasharray="3 3", stroke="rgba(255,255,255,0.05)"),
                            rx.recharts.graphing_tooltip(),
                            data=State.chart_cx,
                            height=220,
                            width="100%",
                        ),
                    ),
                    columns="2",
                    spacing="3",
                    width="100%",
                ),

                # ============================================================
                # ROW 4 — Head of Marketplace horizontal bar (full width)
                # Top 10 sellers by revenue — uses mart_sellers_wide
                # ============================================================
                chart_card(
                    section_header(
                        "Head of Marketplace — Top 10 Sellers by Revenue",
                        "Total item revenue across all delivered orders. Seller IDs truncated for readability.",
                    ),
                    rx.recharts.bar_chart(
                        rx.recharts.bar(
                            data_key="revenue",
                            fill=ACCENT,
                            fill_opacity=0.85,
                            radius=[0, 4, 4, 0],
                        ),
                        rx.recharts.x_axis(data_key="seller", tick={"fill": NEUTRAL, "fontSize": 10}),
                        rx.recharts.y_axis(tick={"fill": NEUTRAL, "fontSize": 11}),
                        rx.recharts.cartesian_grid(stroke_dasharray="3 3", stroke="rgba(255,255,255,0.05)"),
                        rx.recharts.graphing_tooltip(),
                        data=State.chart_sellers,
                        height=220,
                        width="100%",
                        layout="horizontal",
                    ),
                ),

                spacing="4",
                width="100%",
            ),
            padding="2em",
            max_width="1400px",
            margin="0 auto",
        ),

        background_color="#0b0f14",
        min_height="100vh",
        width="100%",
        color="white",
        font_family="'DM Sans', sans-serif",
    )


# ---------------------------------------------------------------------------
# APP
# ---------------------------------------------------------------------------

app = rx.App(
    theme=rx.theme(
        appearance="dark",
        accent_color="orange",
        radius="medium",
    ),
    stylesheets=[
        "https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap",
    ],
)
app.add_page(index, on_load=State.load)