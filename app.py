import streamlit as st
import pandas as pd
from playwright.sync_api import sync_playwright
import os
from datetime import datetime
import time
import asyncio
import sys
import json
import re

# Bug fix for Windows
if sys.platform == 'win32':
    import warnings
    # Silenciar advertencia de depreciación de asyncio en Windows
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Monitor Alojamientos", layout="wide")

# --- CONEXIÓN GOOGLE SHEETS ---
class GSheetsConnection:
    def __init__(self, secrets):
        self.scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        self.secrets = secrets
        self.client = None
        
    def connect(self):
        try:
            # Intentar cargar desde st.secrets (Streamlit Cloud o secrets.toml)
            if "gcp_service_account" in self.secrets:
                creds_dict = dict(self.secrets["gcp_service_account"])
                
                # Fix para saltos de linea en private_key si viene de TOML mal formateado
                if "\\n" in creds_dict["private_key"]:
                    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
                
                credentials = Credentials.from_service_account_info(creds_dict, scopes=self.scope)
                self.client = gspread.authorize(credentials)
                return True
            return False
        except Exception as e:
            # Loguear error pero no bloquear si es fallo de configuración
            print(f"Error conectando a GSheets: {e}")
            return False

    def get_data(self, sheet_name="Reviews"):
        if not self.client: return pd.DataFrame()
        try:
            # Buscar la hoja. Si no existe la crea (o usa la primera)
            try:
                sheet = self.client.open("Base de Datos Reviews").worksheet(sheet_name)
            except:
                sheet = self.client.open("Base de Datos Reviews").sheet1
            
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            # Mejorar debug: imprimir tipo de error y detalles
            err_msg = f"❌ Error GSheets (get_data): {type(e).__name__} - {e}"
            print(err_msg)
            # MOSTRAR ERROR VISIBLE EN LA APP (SOLO DEBUG)
            st.sidebar.error(err_msg)
            if hasattr(e, 'response'):
                st.sidebar.code(f"Response Body: {e.response.text}")
            return pd.DataFrame()

    def save_data(self, df, sheet_name="Reviews"):
        if not self.client: return False
        try:
            try:
                sh = self.client.open("Base de Datos Reviews")
                try: worksheet = sh.worksheet(sheet_name)
                except: worksheet = sh.add_worksheet(title=sheet_name, rows="1000", cols="20")
            except Exception as e:
                st.error(f"No se encontró la hoja 'Base de Datos Reviews'. Asegúrate de haberla creado y compartido con el email del bot.")
                print(f"Error opening sheet: {e}")
                return False

            # Reemplazar NaN con "" para que JSON no falle
            df_clean = df.fillna("")
            worksheet.clear()
            worksheet.update([df_clean.columns.values.tolist()] + df_clean.values.tolist())
            return True
        except Exception as e:
            st.error(f"Error guardando en GSheets: {e}")
            print(f"❌ Error GSheets (save_data): {e}")
            return False

# Inicializar conexión global
try:
    GS_CONN = GSheetsConnection(st.secrets) if "gcp_service_account" in st.secrets else None
except:
    GS_CONN = None

# --- SISTEMA DE ALERTA DE CRISIS ---
CRISIS_KEYWORDS = ["policía", "policia", "denuncia", "robo", "ladrón", "estafa", "chinches", "plaga", "sangre", "moho", "inhabitable", "amenaza", "agresión", "cucaracha"]

def check_crisis(text):
    text_lower = text.lower()
    for kw in CRISIS_KEYWORDS:
        if kw in text_lower:
            return True
    return False

# --- FUNCIONES DE CARGA/GUARDADO ---
json_file = "alojamientos.json"
cleaners_file = "cleaners.json"
csv_file = "historico_reviews.csv"
reviews_csv = "historico_reviews.csv"

def load_reviews_db():
    """Carga la base de datos de reseñas (CSV local o GSheets)."""
    df = pd.DataFrame()

    # 1. Intentar cargar de la Nube (Prioridad)
    if GS_CONN and GS_CONN.connect():
        df_cloud = GS_CONN.get_data()
        # DEBUG VISIBLE EN PANTALLA
        if not df_cloud.empty:
            st.toast(f"☁️ Nube Cargada: {len(df_cloud)} filas", icon="☁️")
            df = df_cloud
        else:
            st.toast("☁️ Nube vacía o error de lectura", icon="⚠️")

    # 2. Fallback: CSV Local (si Nube falló o está vacía)
    if df.empty and os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        st.toast(f"📂 Usando Local: {len(df)} filas", icon="📂")

    # 3. Si sigue vacía, devolver estructura base
    if df.empty:
        st.error("⚠️ DATA ERROR: No se han encontrado datos en Nube ni Local. Ve a Configuración y Repara.")
        return pd.DataFrame(columns=["Date", "Platform", "Name", "Text", "Url", "Hash", "Category", "Cleaner", "Rating"])

    # --- NORMALIZACIÓN Y LIMPIEZA (Aplica a Cloud y Local) ---
    # Asegurar tipos
    if "Date" in df.columns: df["Date"] = pd.to_datetime(df["Date"])
    # ... (Resto igual)
    
    # ...
    
    # Deduplicate
    if "Hash" in df.columns:
        before_dedup = len(df)
        df = df.drop_duplicates(subset=["Hash"], keep="last")
        st.sidebar.caption(f"🧹 Tras Deduplicar: {len(df)} filas (Eliminadas: {before_dedup - len(df)})")

    return df

def save_reviews_db(df):
    """Guarda la base de datos (CSV local y GSheets si está conectado)."""
    # 1. Guardar en Nube (Si hay conexión)
    if GS_CONN and GS_CONN.connect():
        try:
            # Convertir fechas a string para JSON/Sheets
            df_cloud = df.copy()
            if "Date" in df_cloud.columns:
                df_cloud["Date"] = df_cloud["Date"].dt.strftime('%Y-%m-%d %H:%M:%S')
            GS_CONN.save_data(df_cloud)
        except Exception as e:
            print(f"Error saving to cloud: {e}")
        
    # 2. Guardar Local siempre (Backup)
    df.to_csv(reviews_csv, index=False)

def load_cleaners():
    if os.path.exists(cleaners_file):
        with open(cleaners_file, "r") as f:
            try: return json.load(f)
            except: return []
    return []

def save_cleaners(data):
    with open(cleaners_file, "w") as f:
        json.dump(data, f, indent=4)

cleaners = load_cleaners()

# Escaneo Global de Crisis al Inicio
try:
    df_alert = load_reviews_db()
    if not df_alert.empty and "Crisis" in df_alert.columns:
        crisis_items = df_alert[df_alert["Crisis"] == True]
        if not crisis_items.empty:
            st.error(f"🚨 ALERTA DE CRISIS ACCIONABLE: Tienes {len(crisis_items)} problemas críticos sin resolver. Ve a 'Comentarios' urgente.")
            for i, row in crisis_items.iterrows():
                with st.expander(f"🔴 ALERT: {row['Name']} ({row['Platform']})"):
                    st.write(row["Text"])
                    
                    c1, c2 = st.columns(2)
                    if c1.button("✅ Marcar como Resuelto", key=f"crisis_{row['Hash']}"):
                        df_alert.loc[df_alert["Hash"] == row["Hash"], "Crisis"] = False
                        save_reviews_db(df_alert)
                        st.rerun()
except Exception as e:
    st.sidebar.error(f"🚨 Error Crítico en Carga Inicial: {e}")
    # Show traceback for debugging
    import traceback
    st.sidebar.code(traceback.format_exc())

def load_accommodations():
    if os.path.exists(json_file):
        with open(json_file, "r") as f:
            try:
                return json.load(f)
            except:
                return []
    return []

def save_accommodations(data):
    with open(json_file, "w") as f:
        json.dump(data, f, indent=4)

accommodations = load_accommodations()

# --- FUNCIONES DE SCRAPING --- (Resto igual)

# --- FUNCIONES DE SCRAPING ---
def get_listing_data(page, url, platform_type):
    try:
        page.goto(url, timeout=60000)
        page.wait_for_timeout(3000) 
        
        rating = None
        
        if platform_type == "Airbnb":
            try:
                rating_locator = page.get_by_text(re.compile(r"^\d+,\d{2}$")).first
                if rating_locator.count() > 0:
                     rating_text = rating_locator.inner_text()
                else:
                    rating_locator = page.locator('div[data-testid="pdp-reviews-highlight-banner-host-rating"] > div > span > span').first
                    rating_text = rating_locator.inner_text() if rating_locator.count() > 0 else None
            except:
                rating_text = None
            
            if rating_text:
                try:
                    rating = float(rating_text.replace(',', '.').split()[0])
                except:
                    pass

        elif platform_type == "Booking":
            try:
                rating_locator = page.locator('div[data-testid="review-score-component"]').first
                rating_text = rating_locator.inner_text() if rating_locator.count() > 0 else None
                if not rating_text:
                     rating_locator = page.locator('div[data-testid="header-review-score"]').first
                     rating_text = rating_locator.inner_text() if rating_locator.count() > 0 else None
            except:
                rating_text = None

            if rating_text:
                 try:
                    match = re.search(r"(\d+[,.]\d+)", rating_text)
                    if match:
                        rating = float(match.group(1).replace(',', '.'))
                 except:
                    pass
        
        return rating
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

def scrape_data_sync(accommodations_list):
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        })

        progress_text = "Sincronizando notas..."
        my_bar = st.progress(0, text=progress_text)

        tasks = []
        for acc in accommodations_list:
            if acc.get("airbnb"): tasks.append((acc["name"], "Airbnb", acc["airbnb"]))
            if acc.get("booking"): tasks.append((acc["name"], "Booking", acc["booking"]))
            
        total_tasks = len(tasks)
        
        for i, (name, platform, url) in enumerate(tasks):
            rating = get_listing_data(page, url, platform)
            if rating is not None:
                results.append({
                    "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Platform": platform,
                    "Name": name,
                    "URL": url,
                    "Rating": rating
                })
            else:
                pass # Silent fail per individual item to not clutter logic
            
            my_bar.progress((i + 1) / total_tasks, text=f"Procesando {name} ({platform})...")

        browser.close()
        my_bar.empty()
    return results

def get_reviews_for_listing(url, platform):
    reviews = []
    debug_log = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_extra_http_headers({
             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        })
        try:
            page.goto(url, timeout=60000)
            page.wait_for_timeout(4000)
            
            if platform == "Airbnb":
                try:
                    btn = page.locator('[data-testid="pdp-show-all-reviews-button"]').first
                    if btn.count() > 0:
                        btn.evaluate("el => el.click()")
                    else:
                        buttons = (page.locator("button")
                                   .filter(has_text=re.compile(r"evaluaci|review|opinio", re.IGNORECASE))
                                   .all())
                        for b in buttons:
                            if len(b.inner_text()) < 50 and re.search(r"\d+", b.inner_text()):
                                b.evaluate("el => el.click()")
                                break
                except Exception as e:
                    debug_log.append(f"Airbnb click error: {e}")

                page.wait_for_timeout(3000)

                selectors = ['[data-review-id]', 'span[class*="ll4r2nl"]', 'div.r1are2x1']
                reviews_found = []
                for sel in selectors:
                    elements = page.locator(sel).all()
                    if elements:
                        for el in elements[:50]:
                            text = el.inner_text()
                            if len(text) > 30 and text not in reviews_found:
                                reviews_found.append(text)
                        if reviews_found: break
                reviews = reviews_found

            elif platform == "Booking":
                try:
                    btn = page.locator('[data-testid="read-all-actionable"]').first
                    if btn.count() > 0:
                        btn.evaluate("el => el.click()")
                    else:
                         btn_text = page.locator("button").filter(has_text=re.compile(r"Leer todos|See all", re.IGNORECASE)).first
                         if btn_text.count() > 0:
                             btn_text.evaluate("el => el.click()")
                except Exception as e:
                    debug_log.append(f"Booking click error: {e}")

                page.wait_for_timeout(3000)
                
                selectors = ['[data-testid="review-card"]', 'li.review_item']
                reviews_found = []
                for sel in selectors:
                    cards = page.locator(sel).all()
                    if cards:
                        for c in cards[:50]:
                            # Intentar sacar la nota específica
                            score_text = ""
                            try:
                                score_el = c.locator('[data-testid="review-score"]').first
                                if score_el.count() > 0:
                                    score_text = f"⭐ {score_el.inner_text()} | "
                            except: pass

                            txt = c.inner_text()
                            # Filtrado menos agresivo para no borrar cosas útiles
                            lines = [l for l in txt.split('\n') if len(l) > 2] 
                            clean = score_text + "\n".join(lines)
                            
                            if len(clean) > 10:
                                reviews_found.append(clean)
                        if reviews_found: break
                reviews = reviews_found
                
        except Exception as e:
            debug_log.append(str(e))
        
        try:
            browser.close()
        except: pass
        
    return reviews, debug_log


# --- SIDEBAR & NAVEGACIÓN ---
st.sidebar.title("🏨 Monitor Alojamientos")
st.sidebar.caption("v2.0 (Cloud Repair)") # Version Tag for debugging
page_selection = st.sidebar.radio("Ir a:", ["Dashboard", "Comentarios", "Limpieza", "Inteligencia Artificial", "Configuración"])

st.sidebar.markdown("---")
st.sidebar.header("📅 Filtros de Tiempo")
date_filter = st.sidebar.selectbox(
    "Periodo:",
    ["Todo el Histórico", "Última Semana (7 días)", "Último Mes (30 días)", "Último Trimestre (90 días)", "Este Año"]
)
if st.sidebar.button("Aplicar Filtro"):
    st.rerun()

# DEBUG VISUAL PARA EL USUARIO
rows_stat = st.sidebar.empty()
date_range_info = st.sidebar.empty()

def filter_by_date(df, date_col="Date"):
    total_rows = len(df)
    
    # Asegurar datetime (copia para no afectar original fuera si se reusa)
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Mostrar rango real de datos (Debug para usuario)
    if not df.empty:
        min_date = df[date_col].min()
        max_date = df[date_col].max()
        if pd.notna(min_date) and pd.notna(max_date):
             date_range_info.caption(f"📅 Datos desde: {min_date.strftime('%d/%m/%Y')} hasta {max_date.strftime('%d/%m/%Y')}")

    if df.empty or date_filter == "Todo el Histórico":
        rows_stat.info(f"Mostrando: {total_rows} (Todas)")
        return df

    now = datetime.now()
    
    if "Semana" in date_filter:
        cutoff = now - pd.Timedelta(days=7)
    elif "Mes" in date_filter:
        cutoff = now - pd.Timedelta(days=30)
    elif "Trimestre" in date_filter:
        cutoff = now - pd.Timedelta(days=90)
    elif "Este Año" in date_filter:
        cutoff = datetime(now.year, 1, 1)
    else:
        return df
        
    filtered_df = df[df[date_col] >= cutoff]
    rows_stat.info(f"Mostrando: {len(filtered_df)} / {total_rows}")
    return filtered_df

# --- CONFIGURACIÓN DE CONCEPTOS GLOBAL (Para Análisis y Categorización) ---
CONCEPTS_DICT = {
    "Limpieza": {
        "pos": ["limpio", "impecable", "pulcro", "clean", "limpísimo", "brilla"],
        "neg": ["sucio", "polvo", "mancha", "pelo", "dirty", "olor", "insecto", "cucaracha"]
    },
    "Ubicación": {
        "pos": ["ubicación", "location", "cerca", "vistas", "playa", "céntrico", "situación"],
        "neg": ["lejos", "far", "mal situado", "barrio"]
    },
    "Ruido/Descanso": {
        "pos": ["silencioso", "tranquilo", "quiet", "paz", "dormir bien"],
        "neg": ["ruido", "noise", "ralente", "obras", "fiesta", "paredes finas", "tráfico"]
    },
    "Cama/Confort": {
        "pos": ["cómoda", "comfortable", "descanso", "confortable", "almohada bien"],
        "neg": ["incómoda", "dura", "blanda", "colchón", "almohada", "dolor de espalda", "muelles"]
    },
    "Anfitrión/Trato": {
        "pos": ["amable", "atento", "simpático", "host", "help", "ayuda", "rápido"],
        "neg": ["borde", "lento", "grosero", "no contesta", "esperar"]
    },
    "Instalaciones": {
        "pos": ["buen wifi", "internet rápido", "ducha buena", "presión", "bien equipado"],
        "neg": ["wifi", "internet", "agua fría", "no funciona", "roto", "averiado", "cortes", "viejo"]
    },
    "Check-in/Out": {
        "pos": ["fácil", "autónomo", "rápido", "instrucciones claras"],
        "neg": ["llaves", "esperar", "difícil", "no encontré", "lío"]
    }
}
CATEGORIES_LIST = list(CONCEPTS_DICT.keys()) + ["General", "Otros"]

def detect_category(text):
    text_lower = text.lower()
    # Prioridad: Buscar menciones negativas primero, ya que definen la categoría del problema
    for cat, keywords in CONCEPTS_DICT.items():
        for k in keywords["neg"]:
            if k in text_lower: return cat
            
    # Si no, positivas
    for cat, keywords in CONCEPTS_DICT.items():
        for k in keywords["pos"]:
            if k in text_lower: return cat
            
    return "General"

# --- LÓGICA DE INTELIGENCIA ARTIFICIAL ---
def analyze_sentiments(df_reviews):
    """
    Analizador Semántico 'Rule-Based' reutilizando el dict global.
    """
    results = []
    
    # Procesar cada review
    for text in df_reviews["Text"].fillna("").astype(str):
        text_lower = text.lower()
        
        for category, keywords in CONCEPTS_DICT.items():
            # Buscar Positivos
            for word in keywords["pos"]:
                if word in text_lower:
                    results.append({"Category": category, "Type": "Positivo", "Word": word})
                    break # Solo contamos 1 vez por categoría por review
            
            # Buscar Negativos
            for word in keywords["neg"]:
                if word in text_lower:
                    results.append({"Category": category, "Type": "Negativo", "Word": word})
                    break 
                    
    return pd.DataFrame(results) 
                    
    return pd.DataFrame(results)

def generate_smart_reply(review_text, platform, guest_name="Huésped"):
    """
    Genera una respuesta automática basada en el sentimiento detectado.
    """
    df = pd.DataFrame([{"Text": review_text}])
    analysis = analyze_sentiments(df)
    
    greeting = "Hola," if platform == "Airbnb" else "Estimado/a"
    
    if analysis.empty:
        # Respuesta genérica si no detectamos nada específico
        return f"{greeting} {guest_name},\n\nMuchas gracias por tu visita y por tomarte el tiempo de dejarnos una valoración. Esperamos verte pronto de nuevo.\n\nSaludos cordiales."
    
    # Priorizar Negativos
    negatives = analysis[analysis["Type"] == "Negativo"]
    positives = analysis[analysis["Type"] == "Positivo"]
    
    reply = f"{greeting} {guest_name},\n\n"
    
    if not negatives.empty:
        topic = negatives.iloc[0]["Category"]
        reply += f"Lamentamos profundamente que tu experiencia con {topic.lower()} no haya sido perfecta. "
        reply += "Tomamos nota inmediata para revisarlo con nuestro equipo. Queremos ofrecer siempre la máxima calidad.\n\n"
    
    if not positives.empty:
        topic = positives.iloc[0]["Category"]
        if negatives.empty:
            reply += "¡Muchísimas gracias! "
        reply += f"Nos alegra enormemente saber que disfrutaste de {topic.lower()}. "
        if negatives.empty:
            reply += "Trabajamos duro para ello.\n\n"
        else:
            reply += "\n\n"
            
    reply += "Esperamos tener la oportunidad de recibirte de nuevo y ofrecerte una experiencia de 10.\n\nUn saludo."
    return reply

# --- PÁGINA: LIMPIEZA ---
if page_selection == "Limpieza":
    st.title("🧹 Gestión de Limpieza y Equipo")
    st.markdown("Monitoriza el rendimiento de tu staff y asigna tareas.")
    
    # --- GESTIÓN DE EQUIPO (MÓDULO) ---
    with st.expander("👥 Gestionar Miembros del Equipo", expanded=False):
        c_new = st.text_input("Nuevo miembro:", placeholder="Ej: María..")
        if st.button("➕ Añadir"):
            if c_new and c_new not in cleaners:
                cleaners.append(c_new)
                save_cleaners(cleaners)
                st.success(f"Añadido: {c_new}")
                st.rerun()

        st.divider()
        st.caption("Miembros actuales:")
        if not cleaners:
            st.info("No hay equipo definido.")
        else:
            cols = st.columns(3)
            for i, c in enumerate(cleaners):
                with cols[i % 3]:
                    c1, c2 = st.columns([3, 1])
                    c1.text(f"👤 {c}")
                    if c2.button("❌", key=f"del_clean_page_{i}"):
                        cleaners.pop(i)
                        save_cleaners(cleaners)
                        st.rerun()
    
    st.divider()

    # --- DESEMPEÑO DEL EQUIPO ---
    # st.subheader("📊 Desempeño del Equipo") # Eliminado para evitar cabecera vacía
    
    if not cleaners:
        st.warning("⚠️ No tienes equipo configurado. Ve a 'Configuración' para añadir empleados.")
    else:
        df_revs = load_reviews_db()
        # APLICAR FILTRO GLOBAL
        df_revs = filter_by_date(df_revs)
        
        if not df_revs.empty and "Cleaner" in df_revs.columns and "Category" in df_revs.columns:
            # Filtrar solo reviews asignadas
            df_assigned = df_revs[df_revs["Cleaner"].notna()]
            
            if df_assigned.empty:
                st.info("Aún no has asignado limpiezas a ninguna reseña.")
            else:
                # Métricas por Limpiador
                metrics = []
                for cleaner in cleaners:
                    # Total asignado a este cleaner
                    total_assigned = len(df_assigned[df_assigned["Cleaner"] == cleaner])
                    
                    # Quejas de Limpieza (Category="Limpieza")
                    complaints = len(df_assigned[
                        (df_assigned["Cleaner"] == cleaner) & 
                        (df_assigned["Category"] == "Limpieza")
                    ])
                    
                    metrics.append({
                        "Nombre": cleaner,
                        "Asignaciones": total_assigned,
                        "Menciones Limp.": complaints,
                        "Quejas/Total (%)": (complaints/total_assigned * 100) if total_assigned > 0 else 0
                    })
                
                df_metrics = pd.DataFrame(metrics).set_index("Nombre")
                
                # Grafico (Eliminado por petición del usuario - redundante)
                
                # Tabla
                st.subheader("📊 Análisis de Desempeño del Equipo")
                st.write("Menor % de quejas es mejor.")
                
                # Reset index para que "Nombre" sea una columna explicita
                df_display_metrics = df_metrics.reset_index()[["Nombre", "Asignaciones", "Menciones Limp.", "Quejas/Total (%)"]]
                
                event = st.dataframe(
                    df_display_metrics.style.format({"Quejas/Total (%)": "{:.1f}%"}),
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    column_config={
                        "Nombre": "Equipo",
                        "Asignaciones": "Total Rev.",
                        "Menciones Limp.": "Quejas 🧹",
                        "Quejas/Total (%)": "% Malas"
                    }
                )
                
                # --- DETALLE SOLICITADO ---
                st.divider()
                st.subheader("📝 Detalle de Quejas de Limpieza")
                
                # Filtramos las reviews que son de Limpieza
                df_complaints = df_revs[df_revs["Category"] == "Limpieza"].copy()
                
                # LÓGICA DE FILTRADO INTERACTIVO
                selected_cleaner = None
                if event and event.selection["rows"]:
                    idx_selected = event.selection["rows"][0]
                    # Recuperar el nombre usando el índice visual (cuidado con ordenaciones)
                    selected_cleaner = df_display_metrics.iloc[idx_selected]["Nombre"]
                    st.info(f"🔎 Filtrando quejas asignadas a: **{selected_cleaner}**")
                    
                    df_complaints = df_complaints[df_complaints["Cleaner"] == selected_cleaner]

                if not df_complaints.empty:
                    # Seleccionar columnas relevantes
                    df_show = df_complaints[["Date", "Name", "Cleaner", "Text"]].sort_values(by="Date", ascending=False)
                    st.dataframe(
                        df_show, 
                        column_config={
                            "Date": "Fecha",
                            "Name": "Alojamiento", 
                            "Cleaner": "Responsable",
                            "Text": "Comentario"
                        },
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.success("¡No hay quejas de limpieza registradas en este periodo!")
        else:
            st.info("No hay suficientes datos de reseñas todavía para generar estadísticas.")

# --- PÁGINA: INTELIGENCIA ARTIFICIAL ---
if page_selection == "Inteligencia Artificial":
    st.title("🧠 Inteligencia Artificial & Análisis")
    st.markdown("Esta herramienta **lee todas tus opiniones** y detecta automáticamente de qué se quejan o qué aman tus huéspedes.")

    st.divider()
    
    reviews_csv = "historico_reviews.csv"
    if os.path.exists(reviews_csv):
        df_reviews = pd.read_csv(reviews_csv)
        
        if df_reviews.empty:
             st.warning("El historial de opiniones está vacío. Ve a 'Comentarios' y Escanea primero.")
        else:
            # APLICAR FILTRO GLOBAL
            df_reviews = filter_by_date(df_reviews)
            
            st.info(f"Analizando {len(df_reviews)} opiniones ({date_filter})...")
            
            # Analizar
            analysis_df = analyze_sentiments(df_reviews)
            
            if not analysis_df.empty:
                col1, col2 = st.columns(2)
                
                # Agrupar y contar
                counts = analysis_df.groupby(["Category", "Type"]).size().unstack(fill_value=0)
                
                # Asegurar columnas
                if "Positivo" not in counts.columns: counts["Positivo"] = 0
                if "Negativo" not in counts.columns: counts["Negativo"] = 0
                
                # --- LO MÁS AMADO ---
                with col1:
                    st.subheader("😍 Lo que ENAMORA")
                    top_pos = counts["Positivo"].sort_values(ascending=True) # Orden para gráfico barra horizontal
                    st.bar_chart(top_pos, color="#2ecc71", horizontal=True) # Verde
                    
                # --- LO MÁS ODIADO ---
                with col2:
                    st.subheader("😡 Lo que MOLESTA")
                    top_neg = counts["Negativo"].sort_values(ascending=True)
                    st.bar_chart(top_neg, color="#e74c3c", horizontal=True) # Rojo
                    
                st.divider()
                
                # --- CONSEJO IA ---
                st.subheader("💡 Consejo de Actuación")
                worst_category = counts["Negativo"].idxmax()
                count_worst = counts["Negativo"].max()
                total_reviews = len(df_reviews)
                pct = (count_worst / total_reviews) * 100
                
                txt = f"El problema más frecuente es **{worst_category}** (aparece en {count_worst} menciones)."
                
                if worst_category == "Cama/Confort":
                    advice = "Considera invertir en **toppers viscoelásticos** o renovar almohadas. Es la inversión más rentable para subir nota."
                elif worst_category == "Ruido/Descanso":
                    advice = "Mejora el aislamiento o deja tapones de oídos de cortesía con una nota amable."
                elif worst_category == "Limpieza":
                    advice = "Revisa el protocolo con tu equipo de limpieza. Los huéspedes son muy sensibles a pelos y olores."
                elif worst_category == "Instalaciones (Agua/Luz/Wifi)":
                    advice = "Verifica el router o el termo. Un mantenimiento preventivo te ahorrará malas reviews."
                else:
                    advice = "Revisa los comentarios específicos de esta categoría para entender el patrón."
                    
                st.success(f"{txt}\n\n**Recomendación:** {advice}")

            else:
                st.warning("No se detectaron palabras clave en las opiniones actuales.")
    else:
        st.error("No se encontró base de datos de opiniones. Ve a 'Comentarios' y escanea.")

# --- PÁGINA: DASHBOARD ---
if page_selection == "Dashboard":
    st.title("📊 Monitor de Notas")
    
    # DEBUG EXTREMO
    st.write("🔍 DEBUG: Iniciando Carga de Datos...")
    
    # Cargar todos los datos (Cloud o Local)
    try:
        df = load_reviews_db()
        st.write(f"🔍 DEBUG: Datos Cargados. Filas: {len(df)}")
        if not df.empty:
            st.dataframe(df.head())
        else:
            st.error("🔍 DEBUG: DataFrame está VACÍO tras la carga.")
    except Exception as e:
        st.error(f"💀 CRASH cargando DB: {e}")
        df = pd.DataFrame()
    
    if not df.empty:
        if "Name" not in df.columns: df["Name"] = "Desconocido"
        
        # APLICAR FILTRO GLOBAL A NOTAS
        df = filter_by_date(df)
        
        # Filtramos para quedarnos con el ÚLTIMO dato de cada (Nombre, Plataforma)
        df_sorted = df.sort_values(by="Date", ascending=True)
        latest_df = df_sorted.drop_duplicates(subset=["Name", "Platform"], keep="last")
        
        # DEFINICIÓN DE VARIABLES FALTANTES (Rankings)
        airbnb_data = latest_df[latest_df["Platform"] == "Airbnb"]
        booking_data = latest_df[latest_df["Platform"] == "Booking"]
        
        # 1. KPIs Globales (DINÁMICOS POR TIEMPO)
        col1, col2, col3 = st.columns([1, 1, 1])
        
        # Cargar reseñas para calcular media del periodo
        df_kpi_revs = load_reviews_db()
        df_kpi_revs = filter_by_date(df_kpi_revs)
        
        avg_airbnb_period = None
        avg_booking_period = None
        
        if not df_kpi_revs.empty and "Rating" in df_kpi_revs.columns:
            # Asegurar numérico
            df_kpi_revs["Rating"] = pd.to_numeric(df_kpi_revs["Rating"], errors="coerce")
            
            ab_revs = df_kpi_revs[df_kpi_revs["Platform"] == "Airbnb"]
            bk_revs = df_kpi_revs[df_kpi_revs["Platform"] == "Booking"]
            
            if not ab_revs.empty: avg_airbnb_period = ab_revs["Rating"].mean()
            if not bk_revs.empty: avg_booking_period = bk_revs["Rating"].mean()
        
        # Fallback a "Snapshot" si no hay reviews en el periodo (o mostrar guión)
        col1.metric("Media Airbnb (Periodo)", f"{avg_airbnb_period:.2f}" if avg_airbnb_period and not pd.isna(avg_airbnb_period) else "-", border=True, help="Nota media de las reseñas recibidas en este periodo.")
        col2.metric("Media Booking (Periodo)", f"{avg_booking_period:.2f}" if avg_booking_period and not pd.isna(avg_booking_period) else "-", border=True, help="Nota media de las reseñas recibidas en este periodo.")
        
        with col3:
             st.write("") # Spacer
             if st.button("🔄 Sincronizar", use_container_width=True):
                if not accommodations:
                    st.error("Configura primero.")
                else:
                    new_data = scrape_data_sync(accommodations)
                    if new_data:
                        df_new = pd.DataFrame(new_data)
                        header = not os.path.exists(csv_file)
                        df_new.to_csv(csv_file, mode='a', header=header, index=False)
                        st.success(f"Updated!")
                        st.rerun()
                    else:
                        st.warning("Sin datos nuevos.")

        st.divider()
        
        # 2. SECCIÓN CRÍTICA (QUEJAS)
        st.subheader("⚠️ Atención Requerida (Quejas)")
        
        def analyze_review_quality(row):
            """Devuelve (EsNegativa, NotaVisual)"""
            text = row["Text"]
            plat = row["Platform"]
            
            # 1. Booking
            match_bk = re.search(r"[⭐|Puntuación:]\s*(\d+[.,]\d+)", text)
            if match_bk:
                try: 
                    score = float(match_bk.group(1).replace(",", "."))
                    if plat == "Booking":
                        return (score < 7.5, f"{score:.1f}")
                except: pass
            
            # 2. Airbnb
            match_ab = re.search(r"Valoración:\s*(\d+)\s*estrella", text, re.IGNORECASE)
            if match_ab:
                try: 
                    stars = int(match_ab.group(1))
                    if plat == "Airbnb":
                         return (stars <= 3, f"{stars} ⭐")
                except: pass
                
            # 3. Fallback IA
            cat = detect_category(text)
            if cat not in ["General", "Otros"]:
                return (True, "IA Detect")
                
            return (False, "-")

        if os.path.exists(reviews_csv):
            df_all_revs = load_reviews_db()
            # FILTRO GLOBAL
            df_all_revs = filter_by_date(df_all_revs)
            
            if not df_all_revs.empty:
                negative_rows = []
                # Solo analizamos las NEGATIVAS para esta sección
                for i, row in df_all_revs.iterrows():
                    is_neg, score_str = analyze_review_quality(row)
                    if is_neg:
                        r_copy = row.copy()
                        r_copy["Nota"] = score_str
                        negative_rows.append(r_copy)
                
                if negative_rows:
                    df_neg = pd.DataFrame(negative_rows).sort_values(by="Date", ascending=False).head(10)
                    
                    for i, row in df_neg.iterrows():
                        row_hash = row["Hash"]
                        icon = "🅱️" if row["Platform"] == "Booking" else "🅰️"
                        
                        with st.container(border=True):
                            c_head, c_score = st.columns([4, 1])
                            c_head.markdown(f"**{icon} {row['Name']}**  |  📅 {row['Date']}")
                            c_score.error(f"Nota: {row.get('Nota', '-')}")
                            
                            # Limpiar Texto para visualización limpia
                            txt_clean = row["Text"]
                            txt_clean = re.sub(r"Lleva\s+\d+\s+.*?\s+en\s+Airbnb", "", txt_clean, flags=re.IGNORECASE)
                            txt_clean = re.sub(r"Traducido del \w+", "", txt_clean, flags=re.IGNORECASE)
                            txt_clean = re.sub(r"Mostrar el original", "", txt_clean, flags=re.IGNORECASE)
                            # Limpiar nota repetida
                            txt_clean = re.sub(r"Valoraci.n:\s*\d+(?:\.\d)?\s*estrellas", "", txt_clean, flags=re.IGNORECASE)
                            txt_clean = txt_clean.strip(" ,.-·")
                            
                            st.write(txt_clean)
                            
                            c_cat, c_clean = st.columns([1, 1])
                            
                            # Logica Selectores
                            current_matches = df_all_revs[df_all_revs["Hash"] == row_hash]
                            current_cat = "General"
                            current_cleaner = "Sin asignar"
                            
                            if not current_matches.empty:
                                if pd.notna(current_matches.iloc[0].get("Category")):
                                    current_cat = current_matches.iloc[0]["Category"]
                                if pd.notna(current_matches.iloc[0].get("Cleaner")):
                                    current_cleaner = current_matches.iloc[0]["Cleaner"]
                            
                            new_cat = c_cat.selectbox("🏷️ Categoría:", CATEGORIES_LIST, index=CATEGORIES_LIST.index(current_cat) if current_cat in CATEGORIES_LIST else 0, key=f"dash_cat_{row_hash}")
                            
                            opt_clean = ["Sin asignar"] + cleaners
                            try: idx_c = opt_clean.index(current_cleaner)
                            except: idx_c = 0
                            new_cleaner = c_clean.selectbox("🧹 Asignar:", opt_clean, index=idx_c, key=f"dash_clean_{row_hash}")
                                
                            if new_cleaner != current_cleaner or new_cat != current_cat:
                                saved_successfully = False
                                try:
                                    # CRITICAL FIX: Reload FULL DB to avoid deleting filtered rows
                                    df_full_save = load_reviews_db()
                                    
                                    # Ensure Hash is string for matching
                                    df_full_save["Hash"] = df_full_save["Hash"].astype(str)
                                    row_hash_str = str(row_hash)
                                    
                                    if row_hash_str in df_full_save["Hash"].values:
                                        df_full_save.loc[df_full_save["Hash"] == row_hash_str, "Cleaner"] = new_cleaner if new_cleaner != "Sin asignar" else None
                                        df_full_save.loc[df_full_save["Hash"] == row_hash_str, "Category"] = new_cat
                                        save_reviews_db(df_full_save)
                                        saved_successfully = True
                                    else:
                                        st.error(f"Error: No se encontró la reseña con hash {row_hash_str} en la base de datos completa.")
                                except Exception as e:
                                    st.error(f"Error guardando los cambios: {e}")
                                
                                if saved_successfully:
                                    st.toast("Guardado correctamente!")
                                    time.sleep(0.5)
                                    st.rerun()
                else:
                    st.success("✅ No hay quejas críticas en este periodo.")
            
            st.divider()
            
            # 3. ÚLTIMAS OPINIONES (GENERAL)
            st.subheader("💬 Últimas Opiniones (Feed General)")
            if not df_all_revs.empty:
                st.dataframe(
                    df_all_revs[["Date", "Platform", "Name", "Text", "Category", "Cleaner"]].sort_values(by="Date", ascending=False).head(20),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No hay opiniones en este periodo.")
                
        st.divider()
        
        # 4. RANKINGS (AL FINAL)
        with st.expander("🏆 Ver Rankings y Evolución"):
            st.subheader("Top & Bottom Rankings")
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("### Airbnb")
                if not airbnb_data.empty:
                    st.dataframe(airbnb_data.nlargest(5, "Rating")[["Name", "Rating"]], use_container_width=True, hide_index=True)
            with c2:
                st.markdown("### Booking")
                if not booking_data.empty:
                    st.dataframe(booking_data.nlargest(5, "Rating")[["Name", "Rating"]], use_container_width=True, hide_index=True)

        st.divider()
        
        # --- CÁLCULO DE DELTAS Y EVOLUCIÓN ---
        # 1. Preparar datos para Deltas
        df_sorted = df.sort_values(by=["Name", "Platform", "Date"])
        df_sorted["Prev_Rating"] = df_sorted.groupby(["Name", "Platform"])["Rating"].shift(1)
        df_sorted["Delta"] = df_sorted["Rating"] - df_sorted["Prev_Rating"]
        
        # Nos quedamos con la última fila de cada (Name, Platform) que tenga el Delta calculado (o 0 si es el primero)
        latest_status = df_sorted.drop_duplicates(subset=["Name", "Platform"], keep="last").copy()
        
        # Pivotar para tabla
        pivot_rating = latest_status.pivot(index="Name", columns="Platform", values="Rating")
        pivot_delta = latest_status.pivot(index="Name", columns="Platform", values="Delta")
        dates = latest_status.groupby("Name")["Date"].max()
        
        # Unir todo
        final_df = pivot_rating.join(dates).join(pivot_delta, rsuffix="_Delta")
        
        # Columnas seguras
        for col in ["Airbnb", "Booking", "Airbnb_Delta", "Booking_Delta"]:
            if col not in final_df.columns: final_df[col] = None

        # Media para ordenar
        final_df["Media"] = final_df[["Airbnb", "Booking"]].mean(axis=1)
        final_df = final_df.sort_values(by="Media", ascending=False)
        
        # Tabla Principal con Deltas
        st.subheader("📋 Estado Actual y Cambios")
        
        display_df = final_df[["Airbnb", "Airbnb_Delta", "Booking", "Booking_Delta", "Date"]].reset_index()
        
        st.dataframe(
            display_df.style.format({
                "Airbnb": "{:.2f}", 
                "Booking": "{:.1f}",
                "Airbnb_Delta": "{:+.2f}",
                "Booking_Delta": "{:+.1f}"
            }, na_rep="-").map(lambda x: "color: green" if x > 0 else "color: red" if x < 0 else "color: gray", subset=["Airbnb_Delta", "Booking_Delta"]),
            use_container_width=True,
            hide_index=True,
            height=600,
            column_config={
                "Name": "Alojamiento",
                "Date": "Última Actualización",
                "Airbnb": st.column_config.NumberColumn("Airbnb", format="%.2f"),
                "Airbnb_Delta": st.column_config.NumberColumn("Δ Airbnb", help="Cambio respecto a la vez anterior"),
                "Booking": st.column_config.NumberColumn("Booking", format="%.1f"),
                "Booking_Delta": st.column_config.NumberColumn("Δ Booking", help="Cambio respecto a la vez anterior")
            }
        )
        
        st.divider()

        # --- GRÁFICO DE EVOLUCIÓN MENSUAL ---
        st.subheader("📈 Tendencia Mensual Global")
        
        df["Date"] = pd.to_datetime(df["Date"])
        df["Month"] = df["Date"].dt.to_period("M")
        
        # Agrupar por Mes y Plataforma -> Media de notas
        monthly_trends = df.groupby(["Month", "Platform"])["Rating"].mean().reset_index()
        monthly_trends["Month"] = monthly_trends["Month"].dt.to_timestamp()
        
        # Pivotar para gráfico de líneas limpio
        chart_data = monthly_trends.pivot(index="Month", columns="Platform", values="Rating")
        
        st.line_chart(chart_data)

    else:
        st.info("No hay datos históricos. Ve a 'Dashboard' y pulsa 'Sincronizar Ahora'.")

# --- PÁGINA: COMENTARIOS ---
elif page_selection == "Comentarios":
    st.title("💬 Buzón de Opiniones")
    
    
    # (Funciones auxiliares eliminadas porque ya son globales)


    # --- INBOX SECTION ---
    st.markdown("### 📥 Bandeja de Entrada")
    
    # Cargar DB
    # Cargar DB
    df_reviews = load_reviews_db()
    
    # APLICAR FILTRO GLOBAL TAMBIÉN AL INBOX?
    # El usuario dijo "filtros en TODAS las funcionalidades".
    # Aunque sea "Inbox", si quiere ver lo de la "última semana", filtramos.
    if not df_reviews.empty:
        df_reviews = filter_by_date(df_reviews)
    
    if not df_reviews.empty and "New" in df_reviews.columns:
        # Filtrar las marcadas como New
        inbox = df_reviews[df_reviews["New"] == True]
        
        if not inbox.empty:
            st.warning(f"Tienes {len(inbox)} opiniones sin leer.")
            
            if st.button("Marcar todo como leído"):
                df_reviews["New"] = False
                save_reviews_db(df_reviews)
                st.rerun()
            
            for index, row in inbox.iterrows():
                with st.chat_message("user" if row["Platform"]=="Airbnb" else "assistant", avatar="🅰️" if row["Platform"]=="Airbnb" else "🅱️"):
                    st.write(f"**{row['Name']}** ({row['Platform']}) - {row['Date']}")
                    st.text(row["Text"])
                    
                    c1, c2, c3 = st.columns([1, 2, 2])
                    
                    # Botón Mágico
                    if c1.button(f"🪄 Redactar", key=f"btn_inbox_{index}"):
                        reply = generate_smart_reply(row["Text"], row["Platform"])
                        st.code(reply, language="markdown")
                        
                    # Categoría
                    current_cat = row["Category"] if "Category" in row and pd.notna(row["Category"]) else "General"
                    new_cat = c2.selectbox("🏷️ Categoría", CATEGORIES_LIST, index=CATEGORIES_LIST.index(current_cat) if current_cat in CATEGORIES_LIST else 0, key=f"cat_inbox_{index}")
                    
                    if new_cat != current_cat:
                         df_reviews.loc[index, "Category"] = new_cat
                         save_reviews_db(df_reviews)
                         st.rerun()

                    # Asignar Limpieza
                    if cleaners:
                        current_cleaner = row["Cleaner"] if pd.notna(row["Cleaner"]) else "Sin asignar"
                        options = ["Sin asignar"] + cleaners
                        try: idx = options.index(current_cleaner)
                        except: idx = 0
                        
                        selection = c3.selectbox("🧹 Limpieza:", options, index=idx, key=f"clean_inbox_{index}")
                        
                        if selection != current_cleaner:
                            df_reviews.loc[index, "Cleaner"] = selection if selection != "Sin asignar" else None
                            save_reviews_db(df_reviews)
                            st.rerun()

        else:
            st.success("¡Todo al día! No tienes opiniones nuevas pendientes.")
    
    st.divider()

    # --- HISTÓRICO / ON DEMAND ---
    with st.expander("🔎 Consultar Alojamiento Específico"):
        if not accommodations:
            st.warning("Configura alojamientos primero.")
        else:
            labels = [acc["name"] for acc in accommodations]
            selected_name = st.selectbox("Elige Alojamiento:", labels)
            
            if selected_name:
                acc = next((a for a in accommodations if a["name"] == selected_name), None)
                c1, c2 = st.columns(2)
                
                # Helper para procesar On-Demand con persistencia
                def show_review_card(hash_id, text, platform, key_suffix):
                    # Verificar si existe en DB
                    df_db = load_reviews_db()
                    existing_row = df_db[df_db["Hash"] == hash_id]
                    
                    current_cleaner = "Sin asignar"
                    current_cat = "General"
                    
                    if not existing_row.empty:
                        val = existing_row.iloc[0]["Cleaner"]
                        if pd.notna(val): current_cleaner = val
                        cat_val = existing_row.iloc[0]["Category"] if "Category" in existing_row.columns else "General"
                        if pd.notna(cat_val): current_cat = cat_val
                    
                    # --- MOSTRAR TEXTO ---
                    st.info(text)

                    col_a, col_b, col_c = st.columns([1, 1, 1])
                    
                    if col_a.button(f"🪄 Responder", key=f"resp_{key_suffix}"):
                        st.code(generate_smart_reply(text, platform))
                    
                    # Categoría
                    new_cat = col_b.selectbox("🏷️", CATEGORIES_LIST, index=CATEGORIES_LIST.index(current_cat) if current_cat in CATEGORIES_LIST else 0, key=f"cat_{key_suffix}")

                    # Cleaner
                    new_cleaner = current_cleaner
                    if cleaners:
                        options = ["Sin asignar"] + cleaners
                        try: idx = options.index(current_cleaner)
                        except: idx = 0
                        new_cleaner = col_c.selectbox("🧹", options, index=idx, key=f"cl_{key_suffix}")
                        
                    # Save logic
                    if new_cleaner != current_cleaner or new_cat != current_cat:
                            # Guardar en DB!
                            if existing_row.empty:
                                # Insertar nueva
                                new_entry = {
                                    "Hash": hash_id,
                                    "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    "Platform": platform,
                                    "Name": selected_name,
                                    "Text": text,
                                    "New": False, # Ya vista
                                    "Crisis": False,
                                    "Cleaner": new_cleaner if new_cleaner != "Sin asignar" else None,
                                    "Category": new_cat
                                }
                                df_db = pd.concat([df_db, pd.DataFrame([new_entry])], ignore_index=True)
                            else:
                                # Actualizar
                                df_db.loc[df_db["Hash"] == hash_id, "Cleaner"] = new_cleaner if new_cleaner != "Sin asignar" else None
                                df_db.loc[df_db["Hash"] == hash_id, "Category"] = new_cat
                            
                            save_reviews_db(df_db)
                            st.rerun()

                if acc["airbnb"]:
                    st.write("### Airbnb")
                    # Filtrar de DB
                    df_db = load_reviews_db()
                    df_db = filter_by_date(df_db) # Respetar filtro global
                    
                    matches = df_db[
                        (df_db["Name"] == selected_name) & 
                        (df_db["Platform"] == "Airbnb")
                    ].sort_values(by="Date", ascending=False)
                    
                    if not matches.empty:
                         for i, row in matches.iterrows(): 
                            txt = row['Text']
                            rating_val = row.get('Rating')
                            
                            # 1. Limpiar "Lleva X años en Airbnb" (Robustez Unicode/Espacios)
                            # "\w+" pilla "años", "meses", etc. "." pilla caracteres raros si hay encoding.
                            txt = re.sub(r"Lleva\s+\d+\s+.*?\s+en\s+Airbnb", "", txt, flags=re.IGNORECASE)
                            txt = re.sub(r"Traducido del \w+", "", txt, flags=re.IGNORECASE)
                            txt = re.sub(r"Mostrar el original", "", txt, flags=re.IGNORECASE)

                            # 2. Intentar sacar estrellas (Valoración: X estrellas)
                            # Usamos "." en Valoraci.n para ignorar tildes/encoding
                            if pd.isna(rating_val) or str(rating_val) == "?" or str(rating_val) == "nan":
                                match_stars = re.search(r"Valoraci.n:\s*(\d+(?:\.\d)?)\s*estrellas", txt, re.IGNORECASE)
                                if match_stars:
                                    rating_val = match_stars.group(1)
                                    # Limpiar tambien el texto de la valoración
                                    txt = re.sub(r"Valoraci.n:\s*\d+(?:\.\d)?\s*estrellas", "", txt, flags=re.IGNORECASE)

                            # Limpiar comas o puntos locos que queden al principio
                            txt = txt.strip(" ,.-·") # strip caracteres típicos de separador

                            # Formatear bonito
                            rating_display = f"{rating_val}/5" if rating_val and str(rating_val) != "?" else "?"
                            stars_icon = "⭐" * int(float(rating_val)) if rating_val and str(rating_val).replace(".","").isdigit() else "⭐ ?"
                            
                            txt_display = f"**{stars_icon}** ({rating_display})\n\n{txt.strip()}"
                            show_review_card(row["Hash"], txt_display, "Airbnb", f"ab_on_demand_{i}")
                    else:
                        st.info("No hay reseñas registradas para este piso en Airbnb.")
                
                if acc["booking"]:
                    st.write("### Booking")
                    # Filtrar de DB (reusa df_db)
                    matches_bk = df_db[
                        (df_db["Name"] == selected_name) & 
                        (df_db["Platform"] == "Booking")
                    ].sort_values(by="Date", ascending=False)
                    
                    if not matches_bk.empty:
                         for i, row in matches_bk.iterrows(): 
                            txt = row['Text']
                            rating_val = row.get('Rating')
                            
                            # Limpieza Booking si hiciera falta (menos común el texto basura, pero por seacaso)
                            txt = re.sub(r"Comentado el: .*", "", txt, flags=re.IGNORECASE)

                            txt_display = f"⭐ {rating_val if pd.notna(rating_val) else '?'} | {txt.strip()}"
                            show_review_card(row["Hash"], txt_display, "Booking", f"bk_on_demand_{i}")
                    else:
                        st.info("No hay reseñas registradas para este piso en Booking.")
                                
    # --- NUEVA SECCIÓN: HISTORIAL COMPLETO (SOLICITADO) ---
    st.divider()
    with st.expander("📜 Historial Completo de Opiniones (Tabla)"):
        df_full = load_reviews_db()
        df_full = filter_by_date(df_full)
        
        if not df_full.empty:
            st.write(f"Mostrando {len(df_full)} opiniones del periodo seleccionado.")
            st.dataframe(
                df_full[["Date", "Platform", "Name", "Text", "Category", "Cleaner"]].sort_values(by="Date", ascending=False),
                use_container_width=True,
                height=500
            )
        else:
            st.info("No hay datos para mostrar.")

# --- PÁGINA: CONFIGURACIÓN ---
elif page_selection == "Configuración":
    st.subheader("☁️ Base de Datos en la Nube")
    if GS_CONN and GS_CONN.connect():
        st.success("✅ Conectado a Google Sheets")
        
        # Mostrar email para facilitar compartir
        email_bot = st.secrets["gcp_service_account"]["client_email"]
        st.info(f"ℹ️ Asegúrate de compartir tu hoja de cálculo con: `{email_bot}`")
        
    else:
        st.info("Modo Local (Sin conexión a nube). Añade secretos para conectar.")
    
    if os.path.exists(csv_file):
        if st.button("📤 Subir CSV Local a Google Sheets", help="Úsalo una vez para migrar tus datos actuales a la nube."):
            if GS_CONN and GS_CONN.connect():
                df_local = pd.read_csv(csv_file)
                # Ensure dates are strings for sheets
                df_local["Date"] = pd.to_datetime(df_local["Date"]).dt.strftime('%Y-%m-%d %H:%M:%S')
                if GS_CONN.save_data(df_local):
                    st.success("¡Datos migrados a la nube con éxito!")
            else:
                st.error("No se puede conectar. Revisa tu archivo secrets.toml o la configuración.")

    st.title("⚙️ Configuración de Alojamientos")
    
    # --- DEBUG SECTION (Solo para verificar Nube) ---
    with st.expander("🛠️ Debug: Diagnóstico de Nube"):
        if GS_CONN and GS_CONN.connect():
            df_debug = GS_CONN.get_data()
            st.write(f"Filas en Google Sheets: **{len(df_debug)}**")
            if not df_debug.empty:
                st.dataframe(df_debug.head())
            else:
                st.warning("Google Sheets está vacío.")
        else:
            st.error("No se pudo conectar a GSheets para diagnóstico.")
            
    # --- HERRAMIENTA DE REPARACIÓN DE DATOS (NUEVO) ---
    st.divider()
    st.subheader("🚑 Reparación de Base de Datos")
    st.info("Si ves notas extrañas (ej: 456 en vez de 4.5) o columnas faltantes, pulsa este botón.")
    if st.button("🔧 Reparar y Normalizar Datos en Nube"):
        if GS_CONN and GS_CONN.connect():
            df_fix = GS_CONN.get_data()
            if not df_fix.empty:
                # 1. Arreglar Ratings (ej: 456 -> 4.56)
                if "Rating" in df_fix.columns:
                    df_fix["Rating"] = pd.to_numeric(df_fix["Rating"], errors="coerce")
                    # Si es mayor que 10, asumimos que falta dividir por 100 (ej: 456 -> 4.56)
                    # O dividir por 10 (ej: 45 -> 4.5) - Heurística conservadora
                    mask_huge = df_fix["Rating"] > 10
                    df_fix.loc[mask_huge, "Rating"] = df_fix.loc[mask_huge, "Rating"] / 100.0
                    
                # 2. Asegurar Columnas Faltantes
                for col in ["Platform", "Name", "Text", "Url", "Cleaner", "Category", "Hash"]:
                    if col not in df_fix.columns:
                        df_fix[col] = "" if col != "Category" else "General"
                
                # 3. Generar Hash si falta
                import hashlib
                def generate_hash(row):
                    if row.get("Hash") and len(str(row["Hash"])) > 5: return row["Hash"]
                    # Hash basado en texto + fecha + nombre
                    combo = f"{row.get('Date')}{row.get('Name')}{row.get('Text')}"
                    return hashlib.md5(combo.encode('utf-8')).hexdigest()
                
                df_fix["Hash"] = df_fix.apply(generate_hash, axis=1)
                
                # 4. Guardar arreglado
                # Convertir fechas a string para subir
                if "Date" in df_fix.columns:
                    df_fix["Date"] = pd.to_datetime(df_fix["Date"]).dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                GS_CONN.save_data(df_fix)
                st.success("✅ Base de datos reparada y normalizada exitosamente. Recarga la página.")
            else:
                st.warning("La base de datos está vacía, no hay nada que reparar.")
        else:
            st.error("No hay conexión con la Nube.")
    
    st.markdown("Aquí puedes gestionar tu lista de pisos y tu equipo.")
    
    # GESTION DE EQUIPO (MOVIDO A PÁGINA LIMPIEZA)
    # ...


# REPARACIÓN DE FECHAS (NUEVO)
    with st.expander("🛠️ Reparación de Fechas (Avanzado)"):
        st.warning("⚠️ Esto intentará leer la fecha real del texto del comentario y sobrescribir la fecha de registro.")
        if st.button("🔧 Intentar Extraer Fechas Reales"):
            df_fix = load_reviews_db()
            count_fixed = 0
            
            # Helper meses español
            month_map = {
                "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
                "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
            }
            
            # Helper para rating
            def extract_rating(txt, platform):
                # Airbnb
                match_ab = re.search(r"Valoraci.n:\s*(\d+(?:\.\d)?)\s*estrellas", txt, re.IGNORECASE)
                if match_ab: return float(match_ab.group(1))
                
                # Booking
                match_bk = re.search(r"Puntuaci.n:?\s*(\d+(?:[\.,]\d)?)", txt, re.IGNORECASE) # 8,5 or 8.5
                if match_bk:
                    val = match_bk.group(1).replace(",", ".")
                    return float(val) / 2 # Booking is /10, convert to /5? Or keep raw? User wants /5 stars usually.
                    return float(val) 
                return None

            for index, row in df_fix.iterrows():
                    txt = row["Text"]
                    plat = row["Platform"]
                    
                    # --- FECHAS (YA EXISTENTE) ---
                    new_date = None
                    # ... (Existing Date Logic) ...
                    # 1. Airbnb Relativos (Días, Semanas, Meses)
                    match_days = re.search(r"Hace (\d+)\s*días", txt, re.IGNORECASE)
                    if match_days:
                        days_ago = int(match_days.group(1))
                        new_date = datetime.now() - pd.Timedelta(days=days_ago)
                    
                    if not new_date:
                        match_weeks = re.search(r"Hace (\d+)\s*semana", txt, re.IGNORECASE)
                        if match_weeks:
                            w_ago = int(match_weeks.group(1))
                            new_date = datetime.now() - pd.Timedelta(weeks=w_ago)

                    if not new_date:
                        match_months = re.search(r"Hace (\d+)\s*mes", txt, re.IGNORECASE)
                        if match_months:
                            m_ago = int(match_months.group(1))
                            new_date = datetime.now() - pd.Timedelta(days=m_ago*30)
                    
                    # ... [Keep lines 1113-1144 same logic basically, just re-inserting] ...
                    # To minimize diff, I will just add rating logic below the date logic if I can targeting.
                    # But I am replacing the block "for index, row..." so I must provide full body.
                    
                    # 2. Airbnb "X de Mes de Año"
                    if not new_date:
                        match_long = re.search(r"(\d{1,2}) de (\w+) de (\d{4})", txt, re.IGNORECASE)
                        if match_long:
                            try:
                                d, m_txt, y = match_long.groups()
                                m = month_map.get(m_txt.lower(), 1)
                                new_date = datetime(int(y), m, int(d))
                            except: pass
                    
                    # 3. Booking / Genérico
                    if not new_date:
                        match_loose = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", txt, re.IGNORECASE)
                        if match_loose:
                             try:
                                d, m_txt, y = match_loose.groups()
                                m = month_map.get(m_txt.lower())
                                if m: new_date = datetime(int(y), m, int(d))
                             except: pass

                    # 4. Booking "Comentado el: ..."
                    if not new_date:
                        match_bk = re.search(r"Comentado el (\d{1,2}) de (\w+) de (\d{4})", txt, re.IGNORECASE)
                        if match_bk:
                            try:
                                d, m_txt, y = match_bk.groups()
                                m = month_map.get(m_txt.lower(), 1)
                                new_date = datetime(int(y), m, int(d))
                            except: pass

                    if new_date:
                         df_fix.loc[index, "Date"] = new_date.strftime("%Y-%m-%d %H:%M:%S")
                         count_fixed += 1
                    
                    # --- RATING (NUEVO) ---
                    curr_rating = row.get("Rating")
                    if pd.isna(curr_rating) or str(curr_rating) == "" or str(curr_rating) == "nan":
                        r_val = extract_rating(txt, plat)
                        if r_val:
                            df_fix.loc[index, "Rating"] = r_val
            
            if count_fixed > 0:
                save_reviews_db(df_fix)
                st.success(f"¡Hecho! Se han corregido {count_fixed} fechas. ¡Ahora los filtros funcionarán mejor!")
                time.sleep(2)
                st.rerun()
            else:
                st.info("No se encontraron nuevos patrones de fecha.")

    with st.expander("➕ Añadir Nuevo Alojamiento"):
         with st.form("new_acc_form"):
            n = st.text_input("Nombre")
            a_url = st.text_input("Airbnb URL")
            b_url = st.text_input("Booking URL")
            if st.form_submit_button("Guardar"):
                if n:
                    accommodations.append({"name": n, "airbnb": a_url, "booking": b_url})
                    save_accommodations(accommodations)
                    st.success("Guardado!")
                    st.rerun()

    with st.expander("📥 Importación Masiva (Copia/Pega)"):
        st.info("Pega tu lista. Formato libre (detecta URLs automáticamente).")
        bulk = st.text_area("Pega aquí:")
        if st.button("Procesar"):
             lines = bulk.strip().split('\n')
             count = 0
             for line in lines:
                if not line.strip(): continue
                urls = re.findall(r'https?://[^\s]+', line)
                name = line
                for u in urls: name = name.replace(u, "")
                name = name.strip()
                
                ab = ""
                bk = ""
                for u in urls:
                    if "airbnb" in u: ab = u
                    elif "booking" in u: bk = u
                
                if name:
                    accommodations.append({"name": name, "airbnb": ab, "booking": bk})
                    count += 1
             
             save_accommodations(accommodations)
             st.success(f"Importados {count} alojamientos!")
             st.rerun()

    st.subheader(f"Listado Actual ({len(accommodations)})")
    
    for i, acc in enumerate(accommodations):
        c1, c2, c3, c4 = st.columns([3, 3, 3, 1])
        c1.text(acc["name"])
        c2.caption(acc["airbnb"][:40] + "..." if acc["airbnb"] else "-")
        c3.caption(acc["booking"][:40] + "..." if acc["booking"] else "-")
        if c4.button("🗑️", key=f"del_{i}"):
            accommodations.pop(i)
            save_accommodations(accommodations)
            st.rerun()
