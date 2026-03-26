import streamlit as st
import pandas as pd
import pytz
import time
import json
import os
import psutil
from pathlib import Path
from datetime import datetime
from botcity.web.browsers.chrome import default_options
from botcity.web import *
from botcity.plugins.excel import *

# ─────────────────────────────────────────────
# Carrega o dicionário de campos a partir do JSON externo
# ─────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "config" / "campos.json"

def carregar_campo_id_map() -> dict:
    """Lê o arquivo config/campos.json e retorna o dicionário de campos."""
    if not CONFIG_PATH.exists():
        st.error(f"❌ Arquivo de configuração não encontrado: {CONFIG_PATH}")
        st.stop()
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)

TZ_SP = pytz.timezone('America/Sao_Paulo')

# Caminhos fixos do chromium instalado via apt no container
CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
CHROME_BIN        = os.environ.get("CHROME_BIN", "/usr/bin/chromium")


def timestamp_sp():
    return datetime.now(TZ_SP).strftime('%Y-%m-%d %H:%M:%S')


def descartar_alerta(webBot):
    """
    Descarta qualquer alert/confirm/prompt aberto no browser.
    Retorna True se havia um alerta, False se não havia.
    """
    try:
        alert = webBot.driver.switch_to.alert
        texto = alert.text
        alert.accept()
        print(f"[ALERTA DESCARTADO] {texto}")
        return True
    except Exception:
        return False


def fechar_dropdowns_abertos(webBot):
    """Fecha qualquer dropdown Kendo UI aberto clicando fora deles."""
    descartar_alerta(webBot)
    webBot.driver.execute_script("document.body.click();")
    webBot.wait(300)


def selecionar_liberada_para_mvc(webBot, log, id_noticia):
    """
    Seleciona 'Liberada para MVC' no dropdown release-news via JS direto
    no <select> oculto (release-news-select), igual aos campos de Opções
    Adicionais. Funciona mesmo quando o valor já está preenchido.
    """
    OPCAO     = 'Liberada para MVC'
    SELECT_ID = 'release-news-select'

    for tentativa in range(1, 4):
        result = webBot.driver.execute_script(f"""
            var sel = document.getElementById('{SELECT_ID}');
            if (!sel) return 'not_found';
            var valorDesejado = '{OPCAO}';
            for (var i = 0; i < sel.options.length; i++) {{
                if (sel.options[i].text.includes(valorDesejado)) {{
                    sel.selectedIndex = i;
                    sel.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    sel.dispatchEvent(new Event('input',  {{ bubbles: true }}));
                    var kendo = $(sel).data('kendoDropDownList');
                    if (kendo) {{
                        kendo.value(sel.options[i].value);
                        kendo.trigger('change');
                    }}
                    return 'ok';
                }}
            }}
            return 'option_not_found';
        """)

        if result == 'ok':
            webBot.wait(500)
            return True
        elif result == 'not_found':
            log(f"  ⚠️  [{timestamp_sp()}] | ID: {id_noticia} | Tentativa {tentativa}: select '{SELECT_ID}' não encontrado no DOM.")
        else:
            log(f"  ⚠️  [{timestamp_sp()}] | ID: {id_noticia} | Tentativa {tentativa}: opção '{OPCAO}' não encontrada nas options.")

        webBot.wait(1000 * tentativa)

    log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Não foi possível selecionar '{OPCAO}' — continuando sem selecionar.")
    return False


def recuperar_estado(webBot, log, id_noticia):
    """
    Tenta recuperar o estado da página quando algo falha durante o processamento
    de uma notícia — fecha modais abertos via Escape e navega de volta à listagem.
    Evita que falhas num registro contaminem os registros seguintes.
    """
    log(f"  🔁 [{timestamp_sp()}] | ID: {id_noticia} | Recuperando estado da página...")
    try:
        # Descarta alertas pendentes
        descartar_alerta(webBot)
        # Pressiona Escape para fechar modais/overlays
        webBot.driver.find_element(By.TAG_NAME, 'body').send_keys('\ue00c')
        webBot.wait(1000)
        descartar_alerta(webBot)
        # Navega de volta para a listagem do MVC
        webBot.driver.get("https://mvc.boxnet.com.br/")
        webBot.wait(3000)
        webBot.driver.execute_script("if(document.body) document.body.style.zoom='80%'")
        webBot.wait(500)
    except Exception as e:
        log(f"  ⚠️  [{timestamp_sp()}] | ID: {id_noticia} | Erro na recuperação: {e}")


def safe_click(webBot, selector, by, waiting_time=3000, ensure_visible=False, ensure_clickable=False):
    descartar_alerta(webBot)
    el = webBot.find_element(
        selector=selector, by=by,
        waiting_time=waiting_time,
        ensure_visible=ensure_visible,
        ensure_clickable=ensure_clickable
    )
    if el is None:
        return False

    webBot.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
    webBot.wait(200)

    try:
        el.click()
    except Exception:
        descartar_alerta(webBot)
        webBot.driver.execute_script("document.body.click();")
        webBot.wait(300)
        webBot.driver.execute_script("arguments[0].click();", el)

    return True


def clicar_list_mode(webBot):
    for tentativa in range(5):
        el = webBot.find_element(
            selector="list-mode", by=By.ID,
            waiting_time=3000, ensure_visible=False, ensure_clickable=False)
        if el is not None:
            webBot.driver.execute_script("arguments[0].click();", el)
            return True
        webBot.wait(1000)
    return False


def clicar_dropdown_periodo(webBot):
    textos_possiveis = [
        '24 Horas', 'Última Semana', 'Último mês',
        'Últimos 3', 'Últimos 6', 'Último Ano', 'Todo o Período',
    ]
    for texto in textos_possiveis:
        el = webBot.find_element(
            selector=f"//span[contains(@class,'k-input') and contains(normalize-space(text()),'{texto}')]",
            by=By.XPATH, waiting_time=2000,
            ensure_visible=False, ensure_clickable=False
        )
        if el is not None:
            try:
                webBot.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
                webBot.wait(200)
                el.click()
            except Exception:
                try:
                    webBot.driver.execute_script("arguments[0].click();", el)
                except Exception:
                    continue
            return True

    result = webBot.driver.execute_script("""
        var spans = document.querySelectorAll('span.k-input');
        for (var i = 0; i < spans.length; i++) {
            var ancestor = spans[i].closest('.k-widget.k-dropdown');
            if (ancestor && !spans[i].closest('.k-multiselect')) {
                spans[i].click();
                return true;
            }
        }
        return false;
    """)
    return bool(result)


def selecionar_periodo_ultimo_ano(webBot, log, id_noticia):
    for tentativa in range(1, 4):
        abriu = clicar_dropdown_periodo(webBot)
        if not abriu:
            log(f"  ⚠️  [{timestamp_sp()}] | ID: {id_noticia} | Tentativa {tentativa}: dropdown de período não abriu.")
            webBot.wait(1000 * tentativa)
            continue

        webBot.wait(1000)

        el = webBot.find_element(
            selector="//li[contains(normalize-space(text()), 'Último ano') or contains(normalize-space(text()), 'ltimo ano')]",
            by=By.XPATH, waiting_time=2000,
            ensure_visible=False, ensure_clickable=False
        )
        if el is not None:
            try:
                el.click()
            except Exception:
                try:
                    webBot.driver.execute_script("arguments[0].click();", el)
                except Exception:
                    pass
            return True

        result = webBot.driver.execute_script("""
            var items = document.querySelectorAll('.k-list .k-item, ul.k-list-container li');
            for (var i = 0; i < items.length; i++) {
                if (items[i].textContent.toLowerCase().includes('ltimo ano')) {
                    items[i].click();
                    return true;
                }
            }
            return false;
        """)
        if result:
            return True

        log(f"  ⚠️  [{timestamp_sp()}] | ID: {id_noticia} | Tentativa {tentativa}: opção 'Último ano' não encontrada.")
        webBot.wait(1000 * tentativa)

    return False


def buscar_campo_id_noticias(webBot):
    for tentativa in range(3):
        el = webBot.find_element(
            selector="//div[@class='k-multiselect-wrap k-floatwrap']//input",
            by=By.XPATH, waiting_time=2000,
            ensure_visible=True, ensure_clickable=True)
        if el is not None:
            return el
        webBot.wait(1000 * (tentativa + 1))
    return None


def encerrar_sessao(webBot: WebBot):
    """
    Encerra apenas a sessão Chrome desta instância, via PID específico.
    Não afeta sessões de outros usuários.
    """
    pids = []
    try:
        driver_pid = webBot.driver.service.process.pid
        pids.append(driver_pid)
        parent = psutil.Process(driver_pid)
        for child in parent.children(recursive=True):
            pids.append(child.pid)
    except Exception:
        pass

    try:
        webBot.stop_browser()
    except Exception:
        pass

    for pid in pids:
        try:
            psutil.Process(pid).kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    time.sleep(2)


def iniciar_sessao(usuario: str, senha: str) -> WebBot:
    """
    Inicia o Chromium em modo headless usando o binário instalado via apt.
    """
    webBot = WebBot()
    webBot.driver_path = CHROMEDRIVER_PATH
    webBot.browser = Browser.CHROME
    webBot.headless = False

    webBotDef_options = default_options()
    webBotDef_options.binary_location = CHROME_BIN
    webBotDef_options.add_argument("--page-load-strategy=Normal")

    # ── Obrigatório para Docker/Linux ─────────────────────────────────────
    webBotDef_options.add_argument("--headless=new")
    webBotDef_options.add_argument("--no-sandbox")
    webBotDef_options.add_argument("--disable-dev-shm-usage")
    webBotDef_options.add_argument("--disable-gpu")
    webBotDef_options.add_argument("--window-size=1280,1024")

    # ── Anti-throttling ───────────────────────────────────────────────────
    webBotDef_options.add_argument("--disable-background-timer-throttling")
    webBotDef_options.add_argument("--disable-renderer-backgrounding")
    webBotDef_options.add_argument("--disable-backgrounding-occluded-windows")

    # ── Desabilita popup de salvar senha via flags diretas ─────────────────
    # Usamos argumentos diretos em vez de experimental_option/prefs pois o
    # default_options() do BotCity pode sobrescrever as prefs.
    webBotDef_options.add_argument("--disable-save-password-bubble")
    webBotDef_options.add_argument("--disable-features=PasswordManager")
    webBotDef_options.add_argument("--password-store=basic")
    webBotDef_options.add_argument("--use-mock-keychain")

    webBot.options = webBotDef_options
    webBot.browse("https://mvc.boxnet.com.br/Autenticacao/Login?ReturnUrl=%2f")

    webBot.driver.execute_script("if(document.body) document.body.style.zoom='80%'")
    webBot.wait(5000)

    webBot.find_element(
        selector='//*[@id="UserName"]', by=By.XPATH,
        waiting_time=3000, ensure_visible=False, ensure_clickable=False
    ).send_keys(usuario)

    webBot.find_element(
        selector='//*[@id="Password"]', by=By.XPATH,
        waiting_time=1000, ensure_visible=False, ensure_clickable=False
    ).send_keys(senha)

    safe_click(webBot, "/html/body/div/div/form/div[2]/div/button", By.XPATH, 1000)
    webBot.wait(2000)
    webBot.driver.execute_script("if(document.body) document.body.style.zoom='80%'")
    webBot.wait(500)

    safe_click(webBot,
        '//*[@id="headerTodo"]/div/header/div/ul[2]/li[1]/a/div[2]/span',
        By.XPATH, 5000)
    webBot.wait(3000)

    campoPesquisaMVC = webBot.find_element(
        selector="txtPesquisarMvc", by=By.ID,
        waiting_time=10000, ensure_visible=True, ensure_clickable=False)
    if campoPesquisaMVC is None:
        raise RuntimeError("Campo de pesquisa do MVC não encontrado — verifique se o login foi bem sucedido.")
    campoPesquisaMVC.send_keys("IFOOD - BOXNET")

    safe_click(webBot, "//a[contains(text(), 'IFOOD - BOXNET')]", By.XPATH, 10000,
               ensure_visible=True)
    webBot.wait(3000)
    webBot.driver.execute_script("if(document.body) document.body.style.zoom='80%'")
    webBot.wait(500)

    clicar_list_mode(webBot)
    webBot.wait(3000)

    return webBot


def run_bot(df: pd.DataFrame, log_box, usuario: str, senha: str, campo_id_map: dict):
    logs = []

    def log(msg: str):
        logs.append(msg)
        log_box.text('\n'.join(logs))

    start_time = time.time()
    REINICIAR_A_CADA = 20

    # Cada usuário inicia sua própria sessão isolada — sem limpeza global
    webBot = iniciar_sessao(usuario, senha)

    for idx, row in df.iterrows():

        if idx > 0 and idx % REINICIAR_A_CADA == 0:
            log(f"  🔄 [{timestamp_sp()}] | Reiniciando sessão do Chrome ({idx} registros processados)...")
            encerrar_sessao(webBot)
            webBot = iniciar_sessao(usuario, senha)
            log(f"  ✅ [{timestamp_sp()}] | Sessão reiniciada — continuando.")

        # Localiza a coluna de ID de forma case-insensitive
        col_id = next((c for c in df.columns if c.strip().lower() == 'id'), None)
        if col_id is None:
            log(f"  ❌ [{timestamp_sp()}] | Coluna 'Id' não encontrada no arquivo. Colunas disponíveis: {list(df.columns)}")
            break
        id_noticia = str(int(row[col_id]))
        titulo     = row['Titulo']

        log(f"[{timestamp_sp()}] | ID: {id_noticia} | Título: {titulo}")

        descartar_alerta(webBot)

        webBot.driver.execute_script("""
            var btnLimpar = document.getElementById('btnLimparFiltro');
            if (btnLimpar) btnLimpar.click();
        """)
        webBot.wait(300)

        campoBuscaIDnoticias = buscar_campo_id_noticias(webBot)
        if campoBuscaIDnoticias is None:
            webBot.driver.execute_script("""
                var spId = document.getElementById('spIdNoticia');
                if (spId) spId.click();
            """)
            webBot.wait(400)
            campoBuscaIDnoticias = buscar_campo_id_noticias(webBot)

        if campoBuscaIDnoticias is None:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Campo de ID não abriu — pulando.")
            continue

        try:
            campoBuscaIDnoticias.click()
        except Exception:
            try:
                webBot.driver.execute_script("arguments[0].click();", campoBuscaIDnoticias)
            except Exception:
                pass
        webBot.wait(300)
        campoBuscaIDnoticias = buscar_campo_id_noticias(webBot)
        if campoBuscaIDnoticias is None:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Campo perdido após foco — pulando.")
            continue

        campoBuscaIDnoticias.send_keys(id_noticia)
        webBot.wait(500)
        webBot.key_enter(wait=0)
        webBot.wait(500)

        selecionou = selecionar_periodo_ultimo_ano(webBot, log, id_noticia)
        if not selecionou:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Não foi possível selecionar 'Último ano' — pulando.")
            continue
        webBot.wait(500)

        safe_click(webBot, "refresh-results", By.ID, 1000)
        webBot.wait(3000)

        tituloNoticia = webBot.find_element(
            selector="//section[@class='news-content']//h4", by=By.XPATH,
            waiting_time=10000, ensure_visible=True, ensure_clickable=True)

        if tituloNoticia is None:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Notícia não encontrada na listagem — pulando.")
            continue

        webBot.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tituloNoticia)
        webBot.wait(200)
        tituloNoticia.click()
        webBot.wait(2000)

        descartar_alerta(webBot)
        safe_click(webBot, "aditional-options", By.ID, 1000)
        webBot.wait(3000)
        descartar_alerta(webBot)

        for nome_coluna, id_elemento in campo_id_map.items():
            if nome_coluna not in row.index:
                continue

            valor_raw = row[nome_coluna]
            if pd.isna(valor_raw) or str(valor_raw).strip() == '':
                continue

            valor_campo = str(valor_raw).strip()
            id_input    = id_elemento + '-input'
            valor_js    = json.dumps(valor_campo)

            descartar_alerta(webBot)
            safe_click(webBot, id_elemento, By.ID, 5000,
                       ensure_visible=True, ensure_clickable=True)
            webBot.wait(1000)

            webBot.execute_javascript(f"""
var selectOriginal = document.querySelector('select[id="{id_input}"]');
if (selectOriginal) {{
    var valorDesejado = {valor_js};
    for (var i = 0; i < selectOriginal.options.length; i++) {{
        if (selectOriginal.options[i].text.includes(valorDesejado)) {{
            selectOriginal.selectedIndex = i;
            var evChange = new Event('change', {{ bubbles: true }});
            selectOriginal.dispatchEvent(evChange);
            var evInput = new Event('input', {{ bubbles: true }});
            selectOriginal.dispatchEvent(evInput);
            if (typeof $(selectOriginal).data('kendoDropDownList') !== 'undefined') {{
                $(selectOriginal).data('kendoDropDownList').value(selectOriginal.options[i].value);
                $(selectOriginal).data('kendoDropDownList').trigger('change');
            }}
            console.log('Selecionado: ' + valorDesejado);
            break;
        }}
    }}
}} else {{
    console.log('Select não encontrado: {id_input}');
}}
""")
            webBot.wait(5000)
            fechar_dropdowns_abertos(webBot)

        # ── Seleciona "Liberada para MVC" ────────────────────────────────
        descartar_alerta(webBot)
        selecionou_liberada = selecionar_liberada_para_mvc(webBot, log, id_noticia)
        if not selecionou_liberada:
            recuperar_estado(webBot, log, id_noticia)
            continue

        # ── Salva e fecha ─────────────────────────────────────────────────
        descartar_alerta(webBot)
        safe_click(webBot,
            '//*[@id="news-details"]/footer/button[2]',
            By.XPATH, 10000, ensure_visible=True, ensure_clickable=True)
        webBot.wait(5000)

    encerrar_sessao(webBot)

    elapsed = time.time() - start_time
    return elapsed


# ══════════════════════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="RPA iFood", page_icon="🤖", layout="centered")
st.title("🤖 RPA iFood — Atualização em Lote")
st.markdown("---")

st.subheader("🔐 Credenciais MVC")
col1, col2 = st.columns(2)
with col1:
    usuario = st.text_input("Usuário", placeholder="seu.usuario")
with col2:
    senha = st.text_input("Senha", type="password", placeholder="••••••••")

st.markdown("---")

st.subheader("📂 Arquivo de Lote")
uploaded_file = st.file_uploader(
    "Selecione o arquivo XLSX",
    type=["xlsx"],
    help="Arquivo com as colunas: Id, Titulo, Porta-vozes iFood, Nota do iFood, etc."
)

if uploaded_file is not None:
    df = pd.read_excel(uploaded_file, sheet_name="Sheet1")
    # Normaliza nomes de colunas — remove espaços extras e mantém capitalização original
    df.columns = df.columns.str.strip()
    st.success(f"✅ Arquivo carregado com **{len(df)} registros**.")
    st.dataframe(df, use_container_width=True)
    st.markdown("---")

    if not usuario or not senha:
        st.warning("⚠️ Preencha o usuário e a senha antes de iniciar.")
    else:
        if st.button("▶ Iniciar Processamento", type="primary"):
            campo_id_map = carregar_campo_id_map()

            st.markdown("### 📋 Log de Processamento")
            log_box = st.empty()

            with st.spinner("Processando... aguarde."):
                elapsed = run_bot(df, log_box, usuario, senha, campo_id_map)

            minutos = int(elapsed // 60)
            segundos = int(elapsed % 60)
            st.success(
                f"🏁 Processamento concluído! "
                f"Tempo total: **{minutos} min {segundos} s**"
            )