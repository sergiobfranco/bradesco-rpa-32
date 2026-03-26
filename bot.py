import streamlit as st
import pandas as pd
import pytz
import time
import json
from pathlib import Path
from datetime import datetime
from botcity.web.browsers.chrome import default_options
from webdriver_manager.chrome import ChromeDriverManager
from botcity.web import *
from botcity.plugins.excel import *
from selenium.common.exceptions import ElementClickInterceptedException

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


def timestamp_sp():
    return datetime.now(TZ_SP).strftime('%Y-%m-%d %H:%M:%S')


def fechar_dropdowns_abertos(webBot):
    """Fecha qualquer dropdown Kendo UI aberto clicando fora deles."""
    webBot.driver.execute_script("document.body.click();")
    webBot.wait(300)


def safe_click(webBot, selector, by, waiting_time=3000, ensure_visible=False, ensure_clickable=False):
    """
    Localiza e clica em um elemento.
    - Faz scroll até o elemento antes de clicar.
    - Se o clique normal for interceptado, fecha dropdowns e tenta via JS.
    Retorna True se conseguiu, False se não encontrou.
    """
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
    except ElementClickInterceptedException:
        webBot.driver.execute_script("document.body.click();")
        webBot.wait(300)
        webBot.driver.execute_script("arguments[0].click();", el)

    return True


def clicar_list_mode(webBot):
    """Clica no botão modo lista aguardando ele existir no DOM."""
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
    """
    Abre o dropdown de período independente do texto exibido.
    Usa contains() para tolerar classes CSS adicionais e variações de texto.
    """
    textos_possiveis = [
        '24 Horas',
        'Última Semana',
        'Último mês',
        'Últimos 3',
        'Últimos 6',
        'Último Ano',
        'Todo o Período',
    ]
    for texto in textos_possiveis:
        el = webBot.find_element(
            selector=f"//span[contains(@class,'k-input') and contains(normalize-space(text()),'{texto}')]",
            by=By.XPATH, waiting_time=2000,
            ensure_visible=True, ensure_clickable=True
        )
        if el is not None:
            webBot.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
            webBot.wait(200)
            try:
                el.click()
            except ElementClickInterceptedException:
                webBot.driver.execute_script("arguments[0].click();", el)
            return True

    # Fallback JS
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


def selecionar_periodo_ultimo_mes(webBot, log, id_noticia):
    """
    Abre o dropdown de período e seleciona 'Último mês'.
    Tenta até 3 vezes com wait crescente.
    """
    for tentativa in range(1, 4):
        abriu = clicar_dropdown_periodo(webBot)
        if not abriu:
            log(f"  ⚠️  [{timestamp_sp()}] | ID: {id_noticia} | Tentativa {tentativa}: dropdown de período não abriu.")
            webBot.wait(1000 * tentativa)
            continue

        webBot.wait(1000)

        el = webBot.find_element(
            selector="//li[contains(normalize-space(text()), 'Último mês') or contains(normalize-space(text()), 'ltimo m')]",
            by=By.XPATH, waiting_time=2000,
            ensure_visible=True, ensure_clickable=True
        )
        if el is not None:
            try:
                el.click()
            except ElementClickInterceptedException:
                webBot.driver.execute_script("arguments[0].click();", el)
            return True

        # Fallback JS
        result = webBot.driver.execute_script("""
            var items = document.querySelectorAll('.k-list .k-item, ul.k-list-container li');
            for (var i = 0; i < items.length; i++) {
                if (items[i].textContent.toLowerCase().includes('ltimo m')) {
                    items[i].click();
                    return true;
                }
            }
            return false;
        """)
        if result:
            return True

        log(f"  ⚠️  [{timestamp_sp()}] | ID: {id_noticia} | Tentativa {tentativa}: opção 'Último mês' não encontrada.")
        webBot.wait(1000 * tentativa)

    return False


def buscar_campo_id_noticias(webBot):
    """Busca o campo de input de ID com até 3 tentativas."""
    for tentativa in range(3):
        el = webBot.find_element(
            selector="//div[@class='k-multiselect-wrap k-floatwrap']//input",
            by=By.XPATH, waiting_time=5000,
            ensure_visible=True, ensure_clickable=True)
        if el is not None:
            return el
        webBot.wait(1000 * (tentativa + 1))
    return None


def run_bot(df: pd.DataFrame, log_box, usuario: str, senha: str, campo_id_map: dict):
    logs = []

    def log(msg: str):
        logs.append(msg)
        log_box.text('\n'.join(logs))

    start_time = time.time()

    # ── Login ─────────────────────────────────────────────────────────────
    webDriverPath = ChromeDriverManager().install()
    webBot = WebBot()
    webBot.driver_path = webDriverPath
    webBot.browser = Browser.CHROME
    webBot.headless = False
    webBotDef_options = default_options()
    webBotDef_options.add_argument("--page-load-strategy=Normal")
    webBot.options = webBotDef_options
    webBot.browse("https://mvc.boxnet.com.br/Autenticacao/Login?ReturnUrl=%2f")

    webBot.maximize_window()
    webBot.driver.execute_script("document.body.style.zoom='80%'")
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
    webBot.wait(3000)
    webBot.driver.execute_script("document.body.style.zoom='80%'")
    webBot.wait(500)

    # ── Seleciona MVC iFood ───────────────────────────────────────────────
    safe_click(webBot,
        '//*[@id="headerTodo"]/div/header/div/ul[2]/li[1]/a/div[2]/span',
        By.XPATH, 5000)
    webBot.wait(3000)

    campoPesquisaMVC = webBot.find_element(
        selector="txtPesquisarMvc", by=By.ID,
        waiting_time=5000, ensure_visible=True, ensure_clickable=False)
    campoPesquisaMVC.send_keys("IFOOD - BOXNET")

    safe_click(webBot, "//a[contains(text(), 'IFOOD - BOXNET')]", By.XPATH, 10000,
               ensure_visible=True)
    webBot.wait(3000)
    webBot.driver.execute_script("document.body.style.zoom='80%'")
    webBot.wait(500)

    # ── Modo Lista ────────────────────────────────────────────────────────
    clicar_list_mode(webBot)
    webBot.wait(3000)

    # ══════════════════════════════════════════════════════════════════════
    # LOOP PRINCIPAL
    # ══════════════════════════════════════════════════════════════════════
    for idx, row in df.iterrows():

        id_noticia  = str(int(row['Id']))
        titulo      = row['Titulo']
        porta_vozes = str(row['Porta-vozes iFood'])
        nota_ifood  = str(row['Nota do iFood'])

        log(f"[{timestamp_sp()}] | ID: {id_noticia} | Título: {titulo}")

        # ── Limpa filtros via JS (sem scroll) ─────────────────────────────
        webBot.driver.execute_script("""
            var btnLimpar = document.getElementById('btnLimparFiltro');
            if (btnLimpar) btnLimpar.click();
        """)
        webBot.wait(300)

        # ── Abre campo de ID — verifica primeiro se já está aberto ────────
        campoBuscaIDnoticias = buscar_campo_id_noticias(webBot)
        if campoBuscaIDnoticias is None:
            webBot.driver.execute_script("""
                var spId = document.getElementById('spIdNoticia');
                if (spId) spId.click();
            """)
            webBot.wait(600)
            campoBuscaIDnoticias = buscar_campo_id_noticias(webBot)

        if campoBuscaIDnoticias is None:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Campo de ID não abriu — pulando.")
            continue

        campoBuscaIDnoticias.click()
        webBot.wait(300)
        campoBuscaIDnoticias = buscar_campo_id_noticias(webBot)
        if campoBuscaIDnoticias is None:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Campo perdido após foco — pulando.")
            continue

        campoBuscaIDnoticias.send_keys(id_noticia)
        webBot.wait(500)
        webBot.key_enter(wait=0)
        webBot.wait(500)

        # ── Período: Último mês ───────────────────────────────────────────
        selecionou = selecionar_periodo_ultimo_mes(webBot, log, id_noticia)
        if not selecionou:
            log(f"  ❌ [{timestamp_sp()}] | ID: {id_noticia} | Não foi possível selecionar 'Último mês' — pulando.")
            continue
        webBot.wait(1000)

        # ── Refresh ───────────────────────────────────────────────────────
        safe_click(webBot, "refresh-results", By.ID, 1000)
        webBot.wait(3000)

        # ── Abre a notícia ────────────────────────────────────────────────
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

        # ── Menu Opções Adicionais ────────────────────────────────────────
        safe_click(webBot, "aditional-options", By.ID, 1000)
        webBot.wait(5000)

        # ── Itera sobre os campos do dicionário carregado do JSON ─────────
        for nome_coluna, id_elemento in campo_id_map.items():
            if nome_coluna not in row.index:
                continue

            valor_campo  = str(row[nome_coluna])
            id_input     = id_elemento + '-input'

            safe_click(webBot, id_elemento, By.ID, 5000,
                       ensure_visible=True, ensure_clickable=True)
            webBot.wait(1000)

            valor_js = json.dumps(valor_campo)
            webBot.execute_javascript(f"""
setTimeout(function() {{
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
}}, 1000);
""")
            webBot.wait(5000)
            fechar_dropdowns_abertos(webBot)

        # ── Salva e fecha ─────────────────────────────────────────────────
        safe_click(webBot,
            '//*[@id="news-details"]/footer/button[2]',
            By.XPATH, 10000, ensure_visible=True, ensure_clickable=True)
        webBot.wait(5000)

    # ══════════════════════════════════════════════════════════════════════
    elapsed = time.time() - start_time
    return elapsed


# ══════════════════════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="RPA iFood", page_icon="🤖", layout="centered")
st.title("🤖 RPA iFood — Atualização em Lote")
st.markdown("---")

# ── Credenciais ───────────────────────────────────────────────────────────
st.subheader("🔐 Credenciais MVC")
col1, col2 = st.columns(2)
with col1:
    usuario = st.text_input("Usuário", placeholder="seu.usuario")
with col2:
    senha = st.text_input("Senha", type="password", placeholder="••••••••")

st.markdown("---")

# ── Upload do arquivo XLSX ────────────────────────────────────────────────
st.subheader("📂 Arquivo de Lote")
uploaded_file = st.file_uploader(
    "Selecione o arquivo XLSX",
    type=["xlsx"],
    help="Arquivo com as colunas: Id, Titulo, Porta-vozes iFood, Nota do iFood, etc."
)

if uploaded_file is not None:
    df = pd.read_excel(uploaded_file, sheet_name="Sheet1")
    st.success(f"✅ Arquivo carregado com **{len(df)} registros**.")
    st.dataframe(df, use_container_width=True)
    st.markdown("---")

    if not usuario or not senha:
        st.warning("⚠️ Preencha o usuário e a senha antes de iniciar.")
    else:
        if st.button("▶ Iniciar Processamento", type="primary"):
            # Carrega o dicionário do JSON no momento do processamento
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