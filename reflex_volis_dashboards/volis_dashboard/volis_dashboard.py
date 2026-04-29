import reflex as rx
import duckdb

DB_PATH = r"C:\Users\murilo.blum\Documents\projects\volis\db\olist.duckdb"

class State(rx.State):
    # KPIs
    receita_total: str = "R$ 0,00"
    total_pedidos: str = "0"
    ticket_medio: str = "R$ 0,00"
    csat_geral: str = "0.00"
    
    # Dados Gráficos
    dados_cfo: list[dict] = [] 
    dados_coo: list[dict] = [] 
    dados_cmo: list[dict] = [] 
    dados_cxo: list[dict] = [] 

    def carregar_dashboard(self):
        try:
            con = duckdb.connect(DB_PATH)
            
            # CFO: Receita por Mês (Área)
            res_cfo = con.execute("SELECT strftime(order_month, '%Y-%m'), SUM(recognized_revenue) FROM olist.main.mart_orders_wide GROUP BY 1 ORDER BY 1").fetchall()
            self.dados_cfo = [{"name": r[0], "value": round(r[1], 2)} for r in res_cfo]
            
            # COO: On-Time vs Delay (Barras)
            res_coo = con.execute("SELECT is_on_time, COUNT(*) FROM olist.main.mart_orders_wide WHERE is_on_time IS NOT NULL GROUP BY 1").fetchall()
            self.dados_coo = [{"name": "No Prazo" if r[0] else "Atrasado", "value": r[1]} for r in res_coo]
            
            # CMO: Top 5 Estados (Pizza)
            res_cmo = con.execute("SELECT customer_state, COUNT(*) FROM olist.main.mart_orders_wide GROUP BY 1 ORDER BY 2 DESC LIMIT 5").fetchall()
            self.dados_cmo = [{"name": r[0], "value": r[1]} for r in res_cmo]
            
            # CXO: Evolução Review Score (Linha)
            res_cxo = con.execute("SELECT strftime(order_month, '%Y-%m'), AVG(review_score) FROM olist.main.mart_orders_wide GROUP BY 1 ORDER BY 1").fetchall()
            self.dados_cxo = [{"name": r[0], "value": round(r[1], 2)} for r in res_cxo]

            # KPIs Globais
            total_gmv = sum(d["value"] for d in self.dados_cfo)
            total_vols = sum(d["value"] for d in self.dados_coo)
            self.receita_total = f"R$ {total_gmv:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            self.total_pedidos = f"{total_vols:,}".replace(",", ".")
            self.ticket_medio = f"R$ {(total_gmv/total_vols if total_vols > 0 else 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            self.csat_geral = f"{self.dados_cxo[-1]['value']:.2f} ⭐" if self.dados_cxo else "0.00"

            con.close()
        except Exception as e:
            print(f"Erro: {e}")

    def export_pdf(self):
        return rx.call_script("window.print();")

def kpi(label, val):
    return rx.card(rx.vstack(rx.text(label, size="1", color_shade="500"), rx.heading(val, size="5"), align_items="center"), width="100%")

def index():
    return rx.hstack( # CORRIGIDO AQUI
        # Sidebar de Filtros
        rx.vstack(
            rx.heading("Filtros", size="5", margin_bottom="1em"),
            rx.text("Período", size="2"),
            rx.select(["Últimos 12 meses", "2025", "2024"], width="100%"),
            rx.text("Status", size="2", margin_top="1em"),
            rx.select(["Todos", "Entregue", "Cancelado"], width="100%"),
            rx.spacer(),
            rx.button("Sincronizar", on_click=State.carregar_dashboard, color_scheme="ruby", width="100%"),
            rx.button("Gerar PDF", on_click=State.export_pdf, variant="outline", width="100%"),
            width="250px",
            height="100vh",
            padding="2em",
            background_color=rx.color("gray", 2),
            border_right=f"1px solid {rx.color('gray', 4)}",
        ),
        
        # Dashboard Principal
        rx.vstack(
            rx.hstack( # CORRIGIDO AQUI
                rx.heading("Volis C-Level Suite", size="8"),
                rx.spacer(),
                rx.badge("Live Data", color_scheme="green", variant="surface"),
                width="100%", align_items="center", padding_bottom="1em"
            ),
            
            # Row 1: KPIs
            rx.grid(
                kpi("CFO: RECEITA", State.receita_total),
                kpi("COO: PEDIDOS", State.total_pedidos),
                kpi("AOV", State.ticket_medio),
                kpi("CXO: SATISFAÇÃO", State.csat_geral),
                columns="4", spacing="4", width="100%"
            ),

            # Row 2: O Gráfico Mestre (CFO)
            rx.card(
                rx.vstack(
                    rx.text("CFO: Tendência de Faturamento Reconhecido", weight="bold"),
                    rx.recharts.area_chart(
                        rx.recharts.area(data_key="value", stroke="#e93d82", fill="#e93d82", fill_opacity=0.1),
                        rx.recharts.x_axis(data_key="name"),
                        rx.recharts.y_axis(),
                        rx.recharts.graphing_tooltip(),
                        data=State.dados_cfo, height=280, width="100%"
                    ),
                    width="100%"
                ), width="100%"
            ),

            # Row 3: Diversidade (COO, CMO, CXO)
            rx.grid(
                rx.card(rx.vstack(
                    rx.text("COO: Logística (No Prazo)", size="2", weight="bold"),
                    rx.recharts.bar_chart(
                        rx.recharts.bar(data_key="value", fill="#fb7185", radius=[4, 4, 0, 0]),
                        rx.recharts.x_axis(data_key="name"),
                        rx.recharts.graphing_tooltip(),
                        data=State.dados_coo, height=180, width="100%"
                    )
                )),
                rx.card(rx.vstack(
                    rx.text("CMO: Top 5 Estados", size="2", weight="bold"),
                    rx.recharts.pie_chart(
                        rx.recharts.pie(data=State.dados_cmo, data_key="value", name_key="name", cx="50%", cy="50%", outer_radius=60, fill="#e93d82", label=True),
                        rx.recharts.graphing_tooltip(),
                        height=180, width="100%"
                    )
                )),
                rx.card(rx.vstack(
                    rx.text("CXO: Estabilidade do Score", size="2", weight="bold"),
                    rx.recharts.line_chart(
                        rx.recharts.line(data_key="value", stroke="#f43f5e", stroke_width=2),
                        rx.recharts.x_axis(data_key="name"),
                        rx.recharts.graphing_tooltip(),
                        data=State.dados_cxo, height=180, width="100%"
                    )
                )),
                columns="3", spacing="4", width="100%"
            ),
            width="100%", padding="2em", height="100vh", overflow="hidden"
        ),
        width="100%", background_color="#0f1115"
    )

app = rx.App(theme=rx.theme(appearance="dark", accent_color="ruby"))
app.add_page(index)