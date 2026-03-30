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
from db import init_db, gravar_progresso, ler_progresso, gravar_erro, ler_erros, get_ultimo_progresso
import threading
import queue
import io
init_db()

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
    import tempfile

    webBot = WebBot()
    webBot.driver_path = CHROMEDRIVER_PATH
    webBot.browser = Browser.CHROME
    webBot.headless = False

    webBotDef_options = default_options()
    webBotDef_options.binary_location = CHROME_BIN
    webBotDef_options.add_argument("--page-load-strategy=Normal")

    perfil_temp = tempfile.mkdtemp()
    webBotDef_options.add_argument(f"--user-data-dir={perfil_temp}")
    webBotDef_options.add_argument("--profile-directory=Default")

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
    webBotDef_options.add_argument("--disable-features=PasswordManager,AutofillServerCommunication")
    webBotDef_options.add_argument("--password-store=basic")
    webBotDef_options.add_argument("--use-mock-keychain")
    webBotDef_options.add_experimental_option("prefs", {
    "credentials_enable_service": False,
    "profile.password_manager_enabled": False,
    "profile.password_manager_leak_detection": False,
    "autofill.profile_enabled": False,
    })
    webBotDef_options.add_experimental_option("excludeSwitches", ["enable-automation"])

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
    # Fecha popup de salvar senha se aparecer
    try:
        webBot.driver.find_element(By.TAG_NAME, 'body').send_keys('\ue00c')
    except Exception:
        pass
    webBot.wait(500)

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
    campoPesquisaMVC.send_keys("BRADESCO")

    safe_click(webBot, "//a[contains(text(), 'BRADESCO')]", By.XPATH, 10000,
               ensure_visible=True)
    webBot.wait(3000)
    webBot.driver.execute_script("if(document.body) document.body.style.zoom='80%'")
    webBot.wait(500)

    clicar_list_mode(webBot)
    webBot.wait(3000)

    return webBot

def get_df_a_partir_do_ultimo(df: pd.DataFrame, usuario: str) -> pd.DataFrame:
    """Retorna o slice do df a partir do registro seguinte ao último processado."""
    ultimo = get_ultimo_progresso(usuario)
    if ultimo is None:
        return df  # nunca processou nada, começa do início

    col_id = next((c for c in df.columns if c.strip().lower() == 'id'), None)
    if col_id is None:
        return df

    ultimo_id = str(ultimo['ultimo_id'])

    def normalizar_id(val):
        try:
            return str(int(float(str(val))))
        except Exception:
            return str(val).strip()

    mask    = df[col_id].apply(normalizar_id) == ultimo_id
    indices = df[mask].index.tolist()

    if not indices:
        return df  # ID não encontrado, começa do início

    return df.loc[indices[-1] + 1:]

def run_bot(df: pd.DataFrame, log_queue: queue.Queue, usuario: str, senha: str, campo_id_map: dict, resultado: dict):

    def log(msg: str):
        log_queue.put(msg)

    start_time = time.time()
    #REINICIAR_A_CADA = 50

    # Cada usuário inicia sua própria sessão isolada — sem limpeza global
    webBot = iniciar_sessao(usuario, senha)

    for idx, row in df.iterrows():

        #if idx > 0 and idx % REINICIAR_A_CADA == 0:
        #    import random
        #    delay = random.randint(5, 30)
        #    log(f"  🔄 [{timestamp_sp()}] | Reiniciando sessão do Chrome em {delay}s...")
        #    time.sleep(delay)
        #    encerrar_sessao(webBot)
        #    webBot = iniciar_sessao(usuario, senha)

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
            gravar_erro(usuario, id_noticia, titulo, "Campo de ID não abriu")
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
            gravar_erro(usuario, id_noticia, titulo, "Campo perdido após foco")
            continue

        campoBuscaIDnoticias.send_keys(id_noticia)
        webBot.wait(500)
        webBot.key_enter(wait=0)
        webBot.wait(500)

        selecionou = selecionar_periodo_ultimo_ano(webBot, log, id_noticia)
        if not selecionou:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Não foi possível selecionar 'Último ano' — pulando.")
            gravar_erro(usuario, id_noticia, titulo, "Não foi possível selecionar 'Último ano'")
            continue
        webBot.wait(500)

        safe_click(webBot, "refresh-results", By.ID, 1000)
        webBot.wait(3000)

        tituloNoticia = webBot.find_element(
            selector="//section[@class='news-content']//h4", by=By.XPATH,
            waiting_time=10000, ensure_visible=True, ensure_clickable=True)

        if tituloNoticia is None:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Notícia não encontrada na listagem — pulando.")
            gravar_erro(usuario, id_noticia, titulo, "Notícia não encontrada na listagem")
            continue

        webBot.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", tituloNoticia)
        webBot.wait(200)

        # Fecha modal divEdicao se estiver aberto
        webBot.driver.execute_script("""
            var modal = document.getElementById('divEdicao');
            if (modal && modal.style.display !== 'none') {
                modal.style.display = 'none';
            }
        """)
        webBot.wait(300)
        descartar_alerta(webBot)

        try:
            tituloNoticia.click()
        except Exception:
            webBot.driver.execute_script("arguments[0].click();", tituloNoticia)

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

            try:
                valor_campo = str(int(float(str(valor_raw)))).zfill(2)
            except (ValueError, TypeError):
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
            gravar_erro(usuario, id_noticia, titulo, "Não foi possível selecionar 'Liberada para MVC'")
            recuperar_estado(webBot, log, id_noticia)
            continue

        # ── Salva e fecha ─────────────────────────────────────────────────
        descartar_alerta(webBot)
        safe_click(webBot,
            '//*[@id="news-details"]/footer/button[2]',
            By.XPATH, 10000, ensure_visible=True, ensure_clickable=True)
        webBot.wait(5000)
        gravar_progresso(usuario, id_noticia, titulo)
        log(f"  💾 [{timestamp_sp()}] | ID: {id_noticia} | Progresso salvo.")
    encerrar_sessao(webBot)

    resultado['elapsed']  = time.time() - start_time
    resultado['concluido'] = True
    log_queue.put(None)  # sentinela — sinaliza conclusão

def run_bot_with_retry(df: pd.DataFrame, log_queue: queue.Queue,
                       usuario: str, senha: str, campo_id_map: dict,
                       resultado: dict):
    """
    Executa run_bot e, se ele morrer de forma anormal,
    aguarda 5 minutos e retoma a partir do último ID salvo.
    """
    ESPERA_RETOMADA = 300  # 5 minutos
    MAX_TENTATIVAS  = 20
    resultado['start_time'] = time.time()

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        df_fatia = get_df_a_partir_do_ultimo(df, usuario)

        if df_fatia.empty:
            log_queue.put(f"  ✅ [{timestamp_sp()}] | Todos os registros já foram processados.")
            resultado['elapsed']   = time.time() - resultado['start_time']
            resultado['concluido'] = True
            log_queue.put(None)
            return

        if tentativa > 1:
            log_queue.put(
                f"  🔁 [{timestamp_sp()}] | Tentativa {tentativa} — retomando a partir do ID seguinte ao último salvo."
            )

        try:
            run_bot(df_fatia, log_queue, usuario, senha, campo_id_map, resultado)
        except Exception as e:
            log_queue.put(f"  ❌ [{timestamp_sp()}] | Erro inesperado: {e}")

        if resultado.get('concluido'):
            return

        if tentativa < MAX_TENTATIVAS:
            log_queue.put(
                f"  ⏳ [{timestamp_sp()}] | Processamento interrompido. "
                f"Retomando em {ESPERA_RETOMADA // 60} minutos... "
                f"(tentativa {tentativa}/{MAX_TENTATIVAS})"
            )
            time.sleep(ESPERA_RETOMADA)

    log_queue.put(f"  ❌ [{timestamp_sp()}] | Máximo de tentativas atingido. Encerrando.")
    resultado['elapsed'] = time.time() - resultado['start_time']
    log_queue.put(None)

def aguardar_e_iniciar(data_hora: datetime, df: pd.DataFrame, log_queue: queue.Queue,
                        usuario: str, senha: str, campo_id_map: dict, resultado: dict):
    """Fica em espera até data_hora e então dispara o run_bot_with_retry."""
    agora = datetime.now(TZ_SP)
    espera = (data_hora - agora).total_seconds()
    if espera > 0:
        time.sleep(espera)
    run_bot_with_retry(df, log_queue, usuario, senha, campo_id_map, resultado)

# ══════════════════════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════════════════
# ── Estado da sessão ──────────────────────────────────────────────
if 'running'    not in st.session_state: st.session_state.running   = False
if 'logs'       not in st.session_state: st.session_state.logs      = []
if 'log_queue'  not in st.session_state: st.session_state.log_queue = None
if 'thread'     not in st.session_state: st.session_state.thread    = None
if 'resultado'  not in st.session_state: st.session_state.resultado = {}
if 'agendado_em' not in st.session_state: st.session_state.agendado_em = None
if 'aguardando'  not in st.session_state: st.session_state.aguardando  = False

st.set_page_config(page_title="RPA Bradesco (MVC=32)", page_icon="🤖", layout="centered")
st.title("🤖 RPA Bradesco (MVC=32) — Atualização em Lote")
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
    help="Arquivo com as colunas: Id, Titulo, Nivel Bradesco, Ocorrencias Bradesco, etc."
)

if uploaded_file is not None:
    try:
        df = pd.read_excel(uploaded_file, sheet_name="Sheet1")
    except ValueError:
        df = pd.read_excel(uploaded_file, sheet_name=0)
    # Normaliza nomes de colunas — remove espaços extras e mantém capitalização original
    df.columns = df.columns.str.strip()
    st.success(f"✅ Arquivo carregado com **{len(df)} registros**.")
    st.dataframe(df, use_container_width=True)
    st.markdown("---")

    if not usuario or not senha:
            st.warning("⚠️ Preencha o usuário e a senha antes de iniciar.")
        else:
            col_btn, col_agendar = st.columns([1, 1])

            with col_btn:
                if st.button("▶ Iniciar Agora", type="primary",
                            disabled=st.session_state.running or st.session_state.aguardando):
                    campo_id_map = carregar_campo_id_map()
                    st.session_state.logs      = []
                    st.session_state.log_queue = queue.Queue()
                    st.session_state.resultado = {}
                    st.session_state.running   = True
                    st.session_state.aguardando = False

                    t = threading.Thread(
                        target=run_bot_with_retry,
                        args=(df, st.session_state.log_queue, usuario, senha,
                            campo_id_map, st.session_state.resultado),
                        daemon=True
                    )
                    st.session_state.thread = t
                    t.start()

            with col_agendar:
                with st.expander("🕐 Agendar início"):
                    data_agendada = st.date_input(
                        "Data", value=datetime.now(TZ_SP).date())
                    hora_agendada = st.time_input(
                        "Hora (horário de SP)", value=datetime.now(TZ_SP).replace(
                            second=0, microsecond=0).time())

                    if st.button("📅 Confirmar Agendamento",
                                disabled=st.session_state.running or st.session_state.aguardando):
                        dt_agendado = TZ_SP.localize(
                            datetime.combine(data_agendada, hora_agendada))

                        if dt_agendado <= datetime.now(TZ_SP):
                            st.error("❌ A data e hora devem ser no futuro.")
                        else:
                            campo_id_map = carregar_campo_id_map()
                            st.session_state.logs       = []
                            st.session_state.log_queue  = queue.Queue()
                            st.session_state.resultado  = {}
                            st.session_state.aguardando = True
                            st.session_state.agendado_em = dt_agendado

                            t = threading.Thread(
                                target=aguardar_e_iniciar,
                                args=(dt_agendado, df, st.session_state.log_queue,
                                    usuario, senha, campo_id_map,
                                    st.session_state.resultado),
                                daemon=True
                            )
                            st.session_state.thread = t
                            t.start()
                            st.success(
                                f"✅ Agendado para {dt_agendado.strftime('%d/%m/%Y às %H:%M')} (horário de SP)")
                            
            campo_id_map = carregar_campo_id_map()
            st.session_state.logs      = []
            st.session_state.log_queue = queue.Queue()
            st.session_state.resultado = {}
            st.session_state.running   = True

            t = threading.Thread(
                target=run_bot_with_retry,
                args=(df, st.session_state.log_queue, usuario, senha,
                      campo_id_map, st.session_state.resultado),
                daemon=True
            )
            st.session_state.thread = t
            t.start()
    if st.session_state.aguardando and not st.session_state.running:
        dt = st.session_state.agendado_em
        st.info(f"⏳ Processamento agendado para **{dt.strftime('%d/%m/%Y às %H:%M')}** (horário de SP). Aguardando...")

    if st.session_state.running:
        st.markdown("### 📋 Log de Processamento")
        log_box = st.empty()

        with st.spinner("Processando... aguarde."):
            while st.session_state.thread and st.session_state.thread.is_alive():
                try:
                    while True:
                        msg = st.session_state.log_queue.get_nowait()
                        if msg is None:
                            break
                        st.session_state.logs.append(msg)
                except queue.Empty:
                    pass
                log_box.text('\n'.join(st.session_state.logs[-200:]))
                time.sleep(0.5)

        # Drena mensagens finais
        try:
            while True:
                msg = st.session_state.log_queue.get_nowait()
                if msg is not None:
                    st.session_state.logs.append(msg)
                else:
                    break
        except queue.Empty:
            pass
        log_box.text('\n'.join(st.session_state.logs[-200:]))

        st.session_state.running = False
        st.session_state.aguardando = False  # ← adicionar esta linha
        elapsed  = st.session_state.resultado.get('elapsed', 0)
        minutos  = int(elapsed // 60)
        segundos = int(elapsed % 60)
        st.success(
            f"🏁 Processamento concluído! "
            f"Tempo total: **{minutos} min {segundos} s**"
        )

# ── Tabela de progresso ───────────────────────────────────────────
st.markdown("---")
st.subheader("📊 Progresso por Operador")

registros = ler_progresso()
if registros:
    import pandas as pd
    import io
    df_prog = pd.DataFrame(registros, columns=["usuario", "ultimo_id", "ultimo_titulo", "atualizado_em"])
    df_prog.columns = ["Operador", "Último ID", "Último Título", "Atualizado em"]
    st.dataframe(df_prog, use_container_width=True)

    buffer = io.BytesIO()
    df_prog.to_excel(buffer, index=False)
    st.download_button(
        label="⬇️ Baixar tabela em XLSX",
        data=buffer.getvalue(),
        file_name="progresso_operadores.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Nenhum registro de progresso ainda.")     

# ── Tabela de erros ───────────────────────────────────────────────
st.markdown("---")
st.subheader("❌ Registros com Erro")

erros = ler_erros()
if erros:
    df_erros = pd.DataFrame(erros)
    df_erros.columns = ["Operador", "ID Notícia", "Título", "Motivo", "Ocorrido em"]
    st.dataframe(df_erros, use_container_width=True)

    buffer_erros = io.BytesIO()
    df_erros.to_excel(buffer_erros, index=False)
    st.download_button(
        label="⬇️ Baixar erros em XLSX",
        data=buffer_erros.getvalue(),
        file_name="erros_processamento.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.info("Nenhum erro registrado.")           