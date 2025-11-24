# -*- coding: utf-8 -*-
import os, json, re, threading, warnings
from functools import partial
import pymysql
import pandas as pd
import tkinter as tk
from tkinter import messagebox, ttk, filedialog
from tkinter import font as tkfont
from datetime import datetime

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

LOCKED_USER = None   # ou None para liberar geral

# ---------------- Caminho de credenciais ----------------
CRED_FILE_PATH = r"\\fs01\ITAPEVA ATIVAS\DADOS\SA_Credencials.txt"


def load_db_config_from_file(path=CRED_FILE_PATH):
    """
    Lê o arquivo de credenciais (SA_Credencials.txt) e monta o dicionário DB
    usando as chaves:

      GECOBI_HOST
      GECOBI_USER
      GECOBI_PASS
      GECOBI_DB
      GECOBI_PORT
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado:\n{path}")

    cfg = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()

    missing = [k for k in ("GECOBI_HOST", "GECOBI_USER", "GECOBI_PASS", "GECOBI_DB", "GECOBI_PORT")
               if k not in cfg or not cfg[k]]
    if missing:
        raise ValueError(f"Chaves ausentes no arquivo de credenciais: {', '.join(missing)}")

    db = {
        "host": cfg["GECOBI_HOST"],
        "user": cfg["GECOBI_USER"],
        "password": cfg["GECOBI_PASS"],
        "database": cfg["GECOBI_DB"],
        "port": int(cfg["GECOBI_PORT"]),
    }
    return db

# ---------------- Config ----------------
CARTEIRAS = [
    ("Autos - 517", 517),
    ("DivZero - 518", 518),
    ("Cedidas - 519", 519),
]

# Agora o DB vem do arquivo de credenciais
try:
    DB = load_db_config_from_file()
except Exception as e:
    # Se quiser, pode trocar por messagebox + exit, mas como Tk ainda não subiu,
    # vou apenas levantar o erro:
    raise RuntimeError(f"Erro ao carregar credenciais do GECOBI:\n{e}")

PREFS_FILE = "prefs.json"
DEFAULT_THEME = "clam"

# ---------------- SQL ----------------
SQL_BASE = """
WITH acion AS (
SELECT
    cad.cod_cad AS cod_cad1,
    cad.cpfcnpj AS CPFCNPJ1,
    cad.nomecli AS NOMECLI1,
    MAX(his.data_at) AS ultima_data
FROM hist_tb his
JOIN cadastros_tb cad ON cad.cod_cad = his.cod_cli
JOIN stcob_tb     st  ON st.st      = his.ocorr
WHERE cad.cod_cli IN ({in_list})
  AND his.data_at >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 2 MONTH), '%Y-%m-01')
  AND his.data_at  < DATE_ADD(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 1 MONTH)
  AND NULLIF(TRIM(st.bsc), '') IS NOT NULL
  AND cad.stcli <> 'INA'
  AND his.cod_usu <> '999'
GROUP BY cad.cod_cad, cad.cpfcnpj, cad.nomecli
)
SELECT
    cad.cod_cad,
    cad.nmcont AS contrato,
    cad.cpfcnpj,
    cad.nomecli,
    usu.nomeusu,
    CASE WHEN aci.ultima_data IS NULL
         THEN DATE '1900-01-01'
         ELSE aci.ultima_data END AS ultima_data
FROM cadastros_tb cad
JOIN usu_tb usu ON usu.cod_usu = cad.cod_usu
LEFT JOIN acion aci ON cad.cod_cad = aci.cod_cad1
WHERE cad.cod_cli IN ({in_list})
  AND cad.stcli <> 'INA'
{operador_where}
{extra_where}
GROUP BY cad.cod_cad, cad.cpfcnpj, cad.nomecli, usu.nomeusu, aci.ultima_data, cad.nmcont
ORDER BY ultima_data ASC, cad.nomecli ASC;
"""

# Conjuntos de nmcont para filtros opcionais + infos de acordo por contrato
SQL_NMCONT_QR = """
WITH acordos_ranked AS (
SELECT
a.nmcont,
a.cod_aco,
a.data_aco,
a.data_cad,
a.vlr_aco,
a.qtd_p_aco,
a.staco,
ROW_NUMBER() OVER (PARTITION BY a.nmcont ORDER BY a.cod_aco DESC) AS rn_aco
FROM acordos_tb a
WHERE a.cod_cli IN ({in_list})
AND a.data_cad >= '2025-07-01'
),
qtd_aco AS (SELECT nmcont, MAX(rn_aco) AS qtdaco FROM acordos_ranked GROUP BY nmcont)
SELECT DISTINCT aco.nmcont,aco.data_aco,aco.vlr_aco,aco.qtd_p_aco,
qt.qtdaco
FROM acordos_ranked aco
LEFT JOIN qtd_aco qt ON qt.nmcont = aco.nmcont
WHERE aco.rn_aco = 1
AND aco.staco IN ('Q','E');  
"""

SQL_NMCONT_CPC = """
SELECT 
  cad.nmcont,
  MAX(his.data_at) AS dt_ultimo_cpc
FROM cadastros_tb  cad
JOIN hist_tb       his ON his.cod_cli = cad.cod_cad
JOIN stcob_tb      st  ON st.st       = his.ocorr
WHERE cad.cod_cli IN ({in_list})
  AND cad.stcli <> 'INA'
  AND st.bsc LIKE '%CPC%'
  {operador_where}
GROUP BY cad.nmcont;
"""

SQL_NMCONT_NAO = """
SELECT DISTINCT cad.nmcont
FROM cadastros_tb cad
JOIN usu_tb   usu ON usu.cod_usu = cad.cod_usu
WHERE cad.cod_cli IN ({in_list})
  AND cad.stcli <> 'INA'
  AND cad.cod_cad NOT IN (
    SELECT ht.cod_cli
    FROM hist_tb ht
    JOIN stcob_tb st ON st.st = ht.ocorr
    WHERE st.bsc LIKE '%AL%'
      AND ht.cod_usu <> 999
      AND ht.data_at >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 3 MONTH), '%Y-%m-01')
      AND ht.data_at  < DATE_ADD(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL 1 MONTH)
    GROUP BY ht.cod_cli
  )
{operador_where}
GROUP BY cad.nmcont;
"""

# --- NOVO: Perfil do contrato (infoad, comprometimento e flags) ---
SQL_NMCONT_PERFIL = """
WITH perf AS (
  SELECT
    cad.nmcont,
    cad.infoad,
    cad.infoad10 AS comprometimento_credito,
    neg.int_3  AS flag_aposentado,
    neg.int_4  AS flag_bolsafamilia,
    neg.int_7  AS flag_veiculo,
    neg.int_9  AS flag_vinculo_empregaticio,
    neg.int_8  AS flag_obito,
    ROW_NUMBER() OVER (PARTITION BY cad.nmcont ORDER BY cad.cod_cad DESC) AS rn
  FROM cadastros_tb cad
  LEFT JOIN usu_tb usu ON usu.cod_usu = cad.cod_usu
  LEFT JOIN neg_comp_tb neg ON neg.nmcont = cad.nmcont
  WHERE cad.cod_cli IN ({in_list})
    AND cad.stcli <> 'INA'
    {operador_where}
)
SELECT
  nmcont,
  infoad,
  comprometimento_credito,
  flag_aposentado,
  flag_bolsafamilia,
  flag_veiculo,
  flag_vinculo_empregaticio,
  flag_obito
FROM perf
WHERE rn = 1;
"""

# --- E-mails: consulta pontual por cod_cad (lazy) ---
SQL_EMAILS_ONE = """
SELECT
  CASE
    WHEN cad.email IS NULL OR TRIM(cad.email) = '' THEN NULL
    WHEN LOWER(TRIM(cad.email)) LIKE '%%.c' THEN
      CONCAT(SUBSTRING(LOWER(TRIM(cad.email)), 1, CHAR_LENGTH(LOWER(TRIM(cad.email))) - 2), '.com')
    WHEN LOWER(TRIM(cad.email)) LIKE '%%.com.' THEN
      CONCAT(SUBSTRING_INDEX(LOWER(TRIM(cad.email)), '.com', 1), '.com.br')
    WHEN LOWER(TRIM(cad.email)) LIKE '%%.com.b' THEN
      CONCAT(SUBSTRING_INDEX(LOWER(TRIM(cad.email)), '.com', 1), '.com.br')
    WHEN LOWER(TRIM(cad.email)) LIKE '%%.com.r' THEN
      CONCAT(SUBSTRING_INDEX(LOWER(TRIM(cad.email)), '.com', 1), '.com.br')
    ELSE LOWER(TRIM(cad.email))
  END AS email
FROM cadastros_tb cad
WHERE 
  cad.cod_cli IN (517,518,519)
  AND cad.stcli <> 'INA'
  AND cad.cod_cad = %s

UNION ALL

SELECT
  CASE
    WHEN en.endereco IS NULL OR TRIM(en.endereco) = '' THEN NULL
    WHEN LOWER(TRIM(en.endereco)) LIKE '%%.c' THEN
      CONCAT(SUBSTRING(LOWER(TRIM(en.endereco)), 1, CHAR_LENGTH(LOWER(TRIM(en.endereco))) - 2), '.com')
    WHEN LOWER(TRIM(en.endereco)) LIKE '%%.com.' THEN
      CONCAT(SUBSTRING_INDEX(LOWER(TRIM(en.endereco)), '.com', 1), '.com.br')
    WHEN LOWER(TRIM(en.endereco)) LIKE '%%.com.b' THEN
      CONCAT(SUBSTRING_INDEX(LOWER(TRIM(en.endereco)), '.com', 1), '.com.br')
    WHEN LOWER(TRIM(en.endereco)) LIKE '%%.com.r' THEN
      CONCAT(SUBSTRING_INDEX(LOWER(TRIM(en.endereco)), '.com', 1), '.com.br')
    ELSE LOWER(TRIM(en.endereco))
  END AS email
FROM cadastros_tb cad
LEFT JOIN enderecos_tb en ON en.cpfcnpj = cad.cpfcnpj
WHERE 
  cad.cod_cli IN (517,518,519)
  AND cad.stcli <> 'INA'
  AND cad.cod_cad = %s
  AND en.tipo_domicilio = 'M';
"""

# ---------------- Utils ----------------
def fix_email_py(e: str) -> str:
    if not e:
        return ""
    s = e.strip().lower()
    if s.endswith(".c"):
        s = s[:-2] + ".com"
    for sufixo_errado in (".com.", ".com.b", ".com.r"):
        if s.endswith(sufixo_errado):
            s = s[: -len(sufixo_errado)] + ".com.br"
            break
    s = re.sub(r'\.+$', '', s)
    return s

def load_prefs():
    if os.path.exists(PREFS_FILE):
        try:
            with open(PREFS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_prefs(d):
    try:
        with open(PREFS_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
    except:
        pass

def fmt_cpf_cnpj(s):
    s = re.sub(r"\D", "", str(s or ""))
    if len(s) == 11:
        return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"
    if len(s) == 14:
        return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"
    return s

def only_digits(s):
    return re.sub(r"\D", "", str(s or ""))

def cor_por_data(d, dark=False):
    try:
        d = pd.to_datetime(d).date()
    except:
        return "#ffffff" if not dark else "#0f1115"
    hoje = datetime.today().date()
    dias = (hoje - d).days
    if dark:
        if dias <= 7:   return "#13301a"
        if dias <= 30:  return "#2d2615"
        return "#2c1515"
    else:
        if dias <= 7:   return "#e6ffe6"
        if dias <= 30:  return "#fff7e6"
        return "#ffe6e6"

def add_tooltip(widget, text):
    tip = tk.Toplevel(widget); tip.withdraw(); tip.overrideredirect(True)
    lbl = ttk.Label(tip, text=text, relief="solid", borderwidth=1, padding=4); lbl.pack()
    def enter(e): tip.deiconify(); tip.lift(); tip.geometry(f"+{e.x_root+12}+{e.y_root+8}")
    def leave(e): tip.withdraw()
    widget.bind("<Enter>", enter); widget.bind("<Leave>", leave)

def flash_button(btn, ok_text="✔ Copiado"):
    orig = btn.cget("text"); btn.config(text=ok_text)
    btn.after(900, lambda: btn.config(text=orig))

def theme_apply(root, theme_name):
    style = ttk.Style(root)
    names = style.theme_names()
    if theme_name not in names:
        theme_name = "vista" if "vista" in names else ("clam" if "clam" in names else names[0])
    style.theme_use(theme_name)
    prefs = load_prefs(); prefs["theme"] = theme_name; save_prefs(prefs)
    return theme_name

def get_initial_theme(style):
    prefs = load_prefs()
    t = prefs.get("theme", DEFAULT_THEME)
    return t if t in style.theme_names() else DEFAULT_THEME

def get_initial_dark():
    return bool(load_prefs().get("dark_mode", False))

def apply_palette(root, dark: bool):
    style = ttk.Style(root)
    if dark:
        BG, FG = "#0f1115", "#ffffff"
        SUBBG, BORDER, SELBG = "#16181d", "#23262c", "#2a4ea3"
        TREEBG, TREEFG, HEADBG, HEADFG = SUBBG, FG, "#13151a", FG
    else:
        BG, FG = "#ffffff", "#111111"
        SUBBG, BORDER, SELBG = "#f5f6f8", "#d9dbe0", "#e6f0ff"
        TREEBG, TREEFG, HEADBG, HEADFG = "#ffffff", FG, "#f0f1f3", FG

    try: root.configure(bg=BG)
    except: pass

    for cls in ("TFrame","TLabelframe","TLabel","TButton","TCheckbutton","TRadiobutton",
                "TNotebook","TNotebook.Tab","TSeparator","TEntry","TCombobox","TMenubutton"):
        style.configure(cls, background=BG, foreground=FG)

    style.map("TButton",
              background=[("active", SUBBG)],
              foreground=[("disabled", "#9aa0a6" if dark else "#6b7280")])

    style.configure("TEntry", fieldbackground=SUBBG, foreground=FG)
    style.configure("TCombobox", fieldbackground=SUBBG, foreground=FG)

    style.configure("TNotebook", background=BG, bordercolor=BORDER)
    style.configure("TNotebook.Tab", background=SUBBG, foreground=FG,
                    lightcolor=BORDER, bordercolor=BORDER)
    style.map("TNotebook.Tab",
              background=[("selected", BG)],
              foreground=[("selected", FG)])

    style.configure("Treeview",
                    background=TREEBG, fieldbackground=TREEBG,
                    foreground=TREEFG, bordercolor=BORDER)
    style.map("Treeview",
              background=[("selected", SELBG)],
              foreground=[("selected", "#ffffff" if dark else "#000000")])
    style.configure("Treeview.Heading",
                    background=HEADBG, foreground=HEADFG, bordercolor=BORDER)

    style.configure("TSeparator", background=BORDER)

    prefs = load_prefs(); prefs["dark_mode"] = bool(dark); save_prefs(prefs)

# ---------------- Tela Inicial ----------------
class TelaInicial(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Selecionar Carteiras e Operador")
        self.resizable(False, False)

        self.font_base  = tkfont.Font(family="Segoe UI", size=10)
        self.font_title = tkfont.Font(family="Segoe UI", size=11, weight="bold")
        self.font_strong= tkfont.Font(family="Segoe UI", size=10, weight="bold")

        self.style = ttk.Style(self)
        theme_apply(self, get_initial_theme(self.style))
        self.option_add("*Font", self.font_base)
        self.style.configure(".", padding=6)
        self.style.configure("TButton", padding=(10,6))
        self.style.configure("Title.TLabel", font=self.font_title)
        self.style.configure("Strong.TLabel", font=self.font_strong)

        self.dark_var = tk.BooleanVar(value=get_initial_dark())
        apply_palette(self, self.dark_var.get())

        top = ttk.Frame(self); top.grid(row=0, column=0, sticky="ew", padx=12, pady=(10,4))
        ttk.Label(top, text="Selecione as carteiras e (opcional) o operador", style="Title.TLabel").pack(side="left")
        right = ttk.Frame(top); right.pack(side="right")
        ttk.Label(right, text="Tema:").pack(side="left", padx=(0,4))
        self.theme_var = tk.StringVar(value=self.style.theme_use())
        self.theme_box = ttk.Combobox(right, state="readonly", width=12, textvariable=self.theme_var,
                                      values=self.style.theme_names())
        self.theme_box.pack(side="left")
        self.theme_box.bind("<<ComboboxSelected>>",
                            lambda e: (theme_apply(self, self.theme_var.get()),
                                       apply_palette(self, self.dark_var.get())))
        self.dark_chk = ttk.Checkbutton(right, text="🌙 Escuro", variable=self.dark_var,
                                        command=lambda: apply_palette(self, self.dark_var.get()))
        self.dark_chk.pack(side="left", padx=(8,0))

        self.prefs = load_prefs()
        self.vars = []
        pref_carts = set(self.prefs.get("carteiras", []))

        for i, (label, code) in enumerate(CARTEIRAS, start=1):
            v = tk.BooleanVar(value=(code in pref_carts) if pref_carts else (code == 517))
            self.vars.append((v, code, label))
            ttk.Checkbutton(self, text=label, variable=v, command=self._atualizar_operadores)\
                .grid(row=i, column=0, padx=14, pady=4, sticky="w")

        ttk.Separator(self, orient="horizontal").grid(row=len(CARTEIRAS)+1, column=0, sticky="ew", padx=14, pady=(8, 8))

        ttk.Label(self, text="Operador (nomeusu) — opcional:", style="Strong.TLabel")\
            .grid(row=len(CARTEIRAS)+2, column=0, padx=14, pady=(0, 4), sticky="w")
        self.operadores_cbx = ttk.Combobox(self, state="readonly", values=["— carregando —"], width=40)
        self.operadores_cbx.grid(row=len(CARTEIRAS)+3, column=0, padx=14, pady=(0, 10), sticky="w")
        self.operadores_cbx.set("— carregando —")

        bar = ttk.Frame(self); bar.grid(row=len(CARTEIRAS)+4, column=0, padx=14, pady=(0, 14), sticky="ew")
        ttk.Button(bar, text="Cancelar", command=self._cancelar).pack(side="right", padx=(0,6))
        ttk.Button(bar, text="Continuar", command=self._continuar).pack(side="right")

        self.bind("<Return>", lambda e: self._continuar())
        self.bind("<Escape>", lambda e: self._cancelar())

        self._atualizar_operadores()

        self._carteiras = []
        self._operador = None

    def _carteiras_escolhidas(self):
        return [code for v, code, _ in self.vars if v.get()]

    def _buscar_operadores(self, carteiras):
        if not carteiras: return []
        in_list = ",".join(str(c) for c in carteiras)
        sql = f"""
        SELECT DISTINCT TRIM(usu.nomeusu) AS nomeusu
        FROM cadastros_tb cad
        JOIN usu_tb usu ON usu.cod_usu = cad.cod_usu
        WHERE cad.cod_cli IN ({in_list})
        AND cad.stcli <> 'INA'
        ORDER BY 1;""".strip()
        try:
            conn = pymysql.connect(**DB)
            df = pd.read_sql_query(sql, conn); conn.close()
            ops = df["nomeusu"].dropna().astype(str).str.strip().unique().tolist()
            if LOCKED_USER:
                alvo = LOCKED_USER.strip().lower()
                ops = [o for o in ops if o.strip().lower() == alvo]
            return [o for o in ops if o]
        except Exception as e:
            messagebox.showwarning("Aviso", f"Não foi possível carregar operadores:\n{e}")
            return []

    def _atualizar_operadores(self):
        liste = self._carteiras_escolhidas()
        ops = self._buscar_operadores(liste)
        valores = ["— Todos —"] + ops if ops else ["— Todos —"]

        if LOCKED_USER:
            valores = [LOCKED_USER]
            self.operadores_cbx.config(values=valores, state="disabled")
            self.operadores_cbx.set(LOCKED_USER)
        else:
            self.operadores_cbx.config(values=valores, state="readonly")
            preferido = self.prefs.get("operador") or "— Todos —"
            self.operadores_cbx.set(preferido if preferido in valores else valores[0])

    def _continuar(self):
        escolhidas = self._carteiras_escolhidas()
        if not escolhidas:
            messagebox.showwarning("Aviso", "Selecione ao menos uma carteira."); return
        if LOCKED_USER:
            self._operador = LOCKED_USER
        else:
            op = self.operadores_cbx.get().strip()
            self._operador = None if (op == "" or op.startswith("—")) else op
        self._carteiras = escolhidas
        prefs = load_prefs()
        prefs["carteiras"] = escolhidas
        if self._operador: prefs["operador"] = self._operador
        prefs["theme"] = ttk.Style(self).theme_use()
        prefs["dark_mode"] = self.dark_var.get()
        save_prefs(prefs)
        self.destroy()

    def _cancelar(self):
        self._carteiras, self._operador = None, None
        self.destroy()

    @property
    def resultado(self):
        return self._carteiras, self._operador

# ---------------- helpers de flags ----------------
def _fmt_flag(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        v = str(x).strip()
        if v in ("1", "S", "SIM", "True", "true"):
            return "Sim"
        if v in ("0", "N", "NAO", "NÃO", "False", "false"):
            return "Não"
        return v
    except Exception:
        return str(x)

def _fmt_comprometimento(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    try:
        # se já vier em 0-100, detecta e ajusta
        v = float(x)
        if v > 1.5:  # heurística: assume que está em percentual (ex.: 35 -> 35%)
            return f"{v:.0f}%"
        else:
            return f"{v*100:.0f}%"
    except Exception:
        return str(x)

# ---------------- Tela de Dados ----------------
class TelaDados(tk.Tk):
    def __init__(self, carteiras, operador):
        super().__init__()
        self.title("Navegação de Contratos")
        self.resizable(True, True)

        self.font_base  = tkfont.Font(family="Segoe UI", size=10)
        self.font_title = tkfont.Font(family="Segoe UI", size=11, weight="bold")
        self.font_strong= tkfont.Font(family="Segoe UI", size=10, weight="bold")

        self.style = ttk.Style(self)
        theme_apply(self, get_initial_theme(self.style))
        self.option_add("*Font", self.font_base)
        self.style.configure(".", padding=6)
        self.style.configure("TButton", padding=(10,6))
        self.style.configure("Title.TLabel", font=self.font_title)
        self.style.configure("Strong.TLabel", font=self.font_strong)

        self.dark_var = tk.BooleanVar(value=get_initial_dark())
        apply_palette(self, self.dark_var.get())

        self.columnconfigure(0, weight=1)
        self.rowconfigure(7, weight=1)

        self.carteiras = carteiras
        self.operador = operador

        # base cheia e visão filtrada
        self.df_all = pd.DataFrame()
        self.df = pd.DataFrame()
        self.idx = 0
        self._restart = False

        # listas de labels para atualizar cores
        self._detail_title_labels = []
        self._detail_value_labels = []

        # conjuntos de nmcont para filtros
        self.set_qr  = set()  # Quebrado/Rejeitado
        self.set_cpc = set()  # CPC / ACIONAMENTOS / ALÔ
        self.set_nao = set()  # Não acionado (bsc vazio)

        # flags UI dos filtros por nmcont
        self.var_qr  = tk.BooleanVar(value=False)
        self.var_cpc = tk.BooleanVar(value=False)
        self.var_nao = tk.BooleanVar(value=False)

        # cache de e-mails por cod_cad
        self.email_map = {}

        # Header + tema + switch
        hdr = ttk.Frame(self); hdr.grid(row=0, column=0, sticky="ew", padx=12, pady=(12,4))
        ttk.Label(hdr, text="Navegação de Contratos", style="Title.TLabel").pack(side="left")
        right = ttk.Frame(hdr); right.pack(side="right")
        self.lbl_ctx = ttk.Label(right, text=""); self.lbl_ctx.pack(side="left", padx=(0,12))
        ttk.Label(right, text="Tema:").pack(side="left", padx=(0,4))
        self.theme_var = tk.StringVar(value=self.style.theme_use())
        self.theme_box = ttk.Combobox(right, state="readonly", width=12, textvariable=self.theme_var,
                                      values=self.style.theme_names())
        self.theme_box.pack(side="left")
        self.theme_box.bind("<<ComboboxSelected>>",
                            lambda e: (theme_apply(self, self.theme_var.get()),
                                       apply_palette(self, self.dark_var.get()),
                                       self._refresh_detail_colors()))
        self.dark_chk = ttk.Checkbutton(right, text="🌙 Escuro", variable=self.dark_var,
                                        command=self._toggle_dark)
        self.dark_chk.pack(side="left", padx=(8,0))

        # ---- Filtros opcionais por nmcont ----
        flt = ttk.Frame(self); flt.grid(row=1, column=0, sticky="ew", padx=12, pady=(4,0))
        ttk.Label(flt, text="Lista Distribuição", style="Strong.TLabel").pack(side="left", padx=(0,8))
        chk1 = ttk.Checkbutton(flt, text="Quebrado/Rejeitado", variable=self.var_qr)
        chk2 = ttk.Checkbutton(flt, text="CPC", variable=self.var_cpc)
        chk3 = ttk.Checkbutton(flt, text="Não acionado", variable=self.var_nao)
        chk1.pack(side="left"); chk2.pack(side="left", padx=(8,0)); chk3.pack(side="left", padx=(8,0))
        btn_apl = ttk.Button(flt, text="Aplicar filtros", command=self._aplicar_filtros_nmcont)
        btn_lim = ttk.Button(flt, text="Limpar", command=self._limpar_filtros_nmcont)
        btn_apl.pack(side="right")
        btn_lim.pack(side="right", padx=(0,8))
        self.lbl_counts = ttk.Label(flt, text="(carregando conjuntos...)", anchor="e")
        self.lbl_counts.pack(side="right", padx=(12,12))

        # ---- Filtro por COR (verde/amarelo/vermelho) ----
        self.color_var = tk.StringVar(value="todos")
        flt_cor = ttk.Frame(self); flt_cor.grid(row=2, column=0, sticky="ew", padx=12, pady=(4,0))
        ttk.Label(flt_cor, text="Cor:", style="Strong.TLabel").pack(side="left", padx=(0,8))
        for val, txt in [("todos","Todos"), ("verde","Verdes (≤7d)"),
                         ("amarelo","Amarelos (8–30d)"), ("vermelho","Vermelhos (>30d)")]:
            rb = ttk.Radiobutton(flt_cor, text=txt, value=val, variable=self.color_var,
                                 command=self._aplicar_filtros_nmcont)
            rb.pack(side="left", padx=(0,8))

        # Notebook
        self.nb = ttk.Notebook(self)
        self.nb.grid(row=7, column=0, sticky="nsew", padx=12, pady=12)
        self.tab_detalhe = ttk.Frame(self.nb)
        self.tab_lista   = ttk.Frame(self.nb)
        self.nb.add(self.tab_detalhe, text="Detalhe")
        self.nb.add(self.tab_lista, text="Lista")

        # ---- Detalhe ----
        self.tab_detalhe.columnconfigure(0, weight=1)
        self.detail_bg = tk.StringVar(value="#0f1115" if self.dark_var.get() else "#ffffff")
        detail = tk.Frame(self.tab_detalhe, bg=self.detail_bg.get())
        detail.grid(row=0, column=0, sticky="ew", padx=8, pady=8)
        def _rebg(*_): detail.configure(bg=self.detail_bg.get())
        self.detail_bg.trace_add("write", _rebg)

        self._mk_row(detail, "Contrato CPJ:", "contrato", row=0, copy=True, copy_target="contrato")
        self._mk_row(detail, "Usuário:", "usuario", row=1)
        self._mk_row(detail, "Nome do Cliente:", "nome", row=2, copy=True, copy_target="nome")
        self._mk_row(detail, "CPF/CNPJ:", "cpf", row=3, copy=True, copy_target="cpf")
        self._mk_row(detail, "Última Data:", "data", row=4, bold=True)

        # Novas linhas do acordo
        self._mk_row(detail, "Data do Acordo:", "aco_data", row=5)
        self._mk_row(detail, "Valor do Acordo:", "aco_valor", row=6)
        self._mk_row(detail, "Parcelas (Acordo):", "aco_qtd", row=7)
        self._mk_row(detail, "Quantidade de Propostas Formalizadas:", "qtdaco",row=8)

        # Linha adicional: Último CPC
        self._mk_row(detail, "Último CPC:", "cpc_data", row=9, bold=True)

        # --- NOVAS LINHAS: Perfil do contrato ---
        sep1 = ttk.Separator(self.tab_detalhe, orient="horizontal")
        sep1.grid(row=5, column=0, sticky="ew", padx=8, pady=(6,0))

        perfil = tk.Frame(self.tab_detalhe, bg=self.detail_bg.get())
        perfil.grid(row=6, column=0, sticky="ew", padx=8, pady=(0,6))
        def _rebg2(*_): perfil.configure(bg=self.detail_bg.get())
        self.detail_bg.trace_add("write", _rebg2)

        self._mk_row(perfil, "Info Adicional:", "infoad", row=0)
        self._mk_row(perfil, "Comprometimento:", "comprom", row=1, bold=True)
        self._mk_row(perfil, "Aposentado:", "flag_apos", row=2)
        self._mk_row(perfil, "Bolsa Família:", "flag_bolsa", row=3)
        self._mk_row(perfil, "Veículo:", "flag_veic", row=4)
        self._mk_row(perfil, "Vínculo Empregat.:", "flag_vinc", row=5)
        self._mk_row(perfil, "Óbito:", "flag_obito", row=6)

        self._refresh_detail_colors()

        action = ttk.Frame(self.tab_detalhe); action.grid(row=1, column=0, sticky="ew", padx=8, pady=(0,6))
        ttk.Button(action, text="⟵ Voltar à tela inicial", command=self.voltar_inicio).pack(side="left")

        # Botão: ver e-mails do cod_cad atual
        ttk.Button(action, text="📧 Ver e-mails", command=self._mostrar_emails_atual)\
            .pack(side="right")

        export_bar = ttk.Frame(self.tab_detalhe); export_bar.grid(row=2, column=0, sticky="ew", padx=8, pady=(0,6))
        b1 = ttk.Button(export_bar, text="💾 Exportar CSV (Tudo)", command=self.exportar_csv_tudo); b1.pack(side="left")
        b2 = ttk.Button(export_bar, text="🗂️ Exportar Seleção", command=self.exportar_csv_selecao); b2.pack(side="left", padx=(6,0))
        b3 = ttk.Button(export_bar, text="📋 Copiar Detalhe", command=self.copiar_detalhe); b3.pack(side="left", padx=(6,0))
        add_tooltip(b1, "Exporta toda a lista para CSV")
        add_tooltip(b2, "Exporta apenas as linhas selecionadas na aba Lista")
        add_tooltip(b3, "Copia o registro atual (Detalhe) como CSV")

        nav = ttk.Frame(self.tab_detalhe)
        nav.grid(row=3, column=0, sticky="ew", padx=8, pady=(0,6))

        self.btn_prev = ttk.Button(nav, text="« Anterior", width=16, command=self.anterior)
        self.btn_next = ttk.Button(nav, text="Próximo »", width=16, command=self.proximo)

        self.btn_next.pack(side="right", padx=(6,0))
        self.btn_prev.pack(side="right")

        goto = ttk.Frame(self.tab_detalhe); goto.grid(row=4, column=0, sticky="ew", padx=8, pady=(0,6))
        ttk.Label(goto, text="Ir para #").pack(side="left")
        self.idx_var = tk.IntVar(value=1)
        ent = ttk.Entry(goto, textvariable=self.idx_var, width=8); ent.pack(side="left", padx=(6,8))
        ttk.Button(goto, text="Ir", command=lambda: self._goto(self.idx_var.get()-1)).pack(side="left")

        # ---- Lista ----
        self.tab_lista.rowconfigure(0, weight=1)
        self.tab_lista.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            self.tab_lista,
            columns=("contrato","nome","cpf","usuario","dt","aco_dt","aco_vlr","aco_qtd"),
            show="headings", height=14
        )
        for c, w in [
            ("contrato",120),
            ("nome",300),
            ("cpf",160),
            ("usuario",180),
            ("dt",110),
            ("aco_dt",110),
            ("aco_vlr",120),
            ("aco_qtd",90),
        ]:
            self.tree.heading(c, text=c.upper())
            self.tree.column(c, width=w, anchor="w")

        self.tree.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self.tree.bind("<Double-1>", self._ir_para_detalhe_por_duplo_clique)

        # configurar tags de cor para as linhas
        self._setup_tree_tags()

        # Status
        self.status = ttk.Label(self, text="Pronto", anchor="w")
        self.status.grid(row=99, column=0, sticky="ew", padx=8, pady=(0,6))

        # Atalhos
        self.bind("<Left>", lambda e: self.anterior())
        self.bind("<Right>", lambda e: self.proximo())
        self.bind("<Control-c>", lambda e: self._copy_current_cpf())
        self.bind("<Control-Shift-C>", lambda e: self._copy_current_nome())

        # Carregar dados + conjuntos
        self._carregar_dados_e_conjuntos_async()

    # ---- Email: busca pontual + popup ----
    def _fetch_emails_by_cod(self, cod_cad: str):
        """Retorna lista de e-mails (deduplicada, case-insensitive) para um cod_cad."""
        if not cod_cad:
            return []
        if cod_cad in self.email_map:
            return self.email_map[cod_cad]

        try:
            conn = pymysql.connect(**DB)
            cur = conn.cursor()
            cur.execute(SQL_EMAILS_ONE, (cod_cad, cod_cad))
            rows = cur.fetchall()
            conn.close()

            emails = []
            for r in rows or []:
                e = (r[0] or "").strip()
                if e:
                    e = fix_email_py(e)
                    emails.append(e)

            seen = set()
            uniq = []
            for e in emails:
                k = e.lower()
                if k not in seen:
                    seen.add(k)
                    uniq.append(e)

            self.email_map[cod_cad] = uniq
            return uniq
        except Exception:
            self.email_map[cod_cad] = []
            return []

    def _mostrar_emails_atual(self):
        if self.df.empty:
            messagebox.showinfo("E-mails", "Sem registro selecionado.")
            return

        row = self.df.iloc[self.idx]
        cod = str(row.get("cod_cad", "")).strip()
        emails = self._fetch_emails_by_cod(cod)

        win = tk.Toplevel(self)
        win.title("E-mails do cliente")
        win.transient(self)
        win.grab_set()
        win.resizable(True, True)

        frm = ttk.Frame(win, padding=10)
        frm.pack(fill="both", expand=True)
        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(1, weight=1)

        ttk.Label(
            frm,
            text="Selecione um ou mais e-mails para copiar (Enter/double-click).",
            style="Strong.TLabel"
        ).grid(row=0, column=0, sticky="w", pady=(0,6))

        lst_wrap = ttk.Frame(frm)
        lst_wrap.grid(row=1, column=0, sticky="nsew")
        lst_wrap.columnconfigure(0, weight=1)
        lst_wrap.rowconfigure(0, weight=1)

        try:
            f = self.font_base
        except Exception:
            f = tkfont.Font(family="Segoe UI", size=10)
        max_len = max((len(e) for e in emails), default=18)
        width_chars = min(max(28, max_len + 2), 80)
        height_rows = min(max(4, len(emails) if emails else 1), 14)

        lb = tk.Listbox(
            lst_wrap,
            selectmode="extended",
            height=height_rows,
            width=width_chars,
            activestyle="dotbox",
            font=f
        )
        vsb = ttk.Scrollbar(lst_wrap, orient="vertical", command=lb.yview)
        lb.configure(yscrollcommand=vsb.set)
        lb.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        if emails:
            for e in emails:
                lb.insert("end", e)
        else:
            lb.insert("end", "(sem e-mails)")

        btn_bar = ttk.Frame(frm)
        btn_bar.grid(row=2, column=0, sticky="ew", pady=(10,0))
        btn_bar.columnconfigure(0, weight=1)

        def copy_selected(*_):
            if not emails:
                return
            idxs = lb.curselection()
            sel = [lb.get(i) for i in idxs]
            if not sel:
                if lb.size() and not lb.get(0).startswith("("):
                    sel = [lb.get(lb.index("active"))]
            if sel and not sel[0].startswith("("):
                self._copy_to_clipboard("\n".join(sel))

        def copy_all():
            if emails:
                self._copy_to_clipboard("\n".join(emails))

        left = ttk.Frame(btn_bar)
        left.grid(row=0, column=0, sticky="w")
        ttk.Button(left, text="Copiar selecionados", command=copy_selected).pack(side="left")
        ttk.Button(left, text="Copiar todos", command=copy_all).pack(side="left", padx=(6,0))

        ttk.Button(btn_bar, text="Fechar", command=win.destroy).grid(row=0, column=1, sticky="e")

        lb.bind("<Return>", copy_selected)
        lb.bind("<Double-1>", copy_selected)
        lb.bind("<Control-a>", lambda e: (lb.select_set(0, "end"), "break"))
        lb.focus_set()

    # ---- helpers de cor ----
    def _current_fg(self):
        return "#ffffff" if self.dark_var.get() else "#111111"

    def _update_detail_bgs(self, bg):
        for lbl in self._detail_value_labels + self._detail_title_labels:
            try:
                lbl.config(bg=bg)
            except:
                pass

    def _refresh_detail_colors(self):
        fg_values = "#ffffff" if self.dark_var.get() else "#111111"
        fg_titles = "#ffffff" if self.dark_var.get() else "#111111"
        bg_now    = self.detail_bg.get()
        for lbl in self._detail_value_labels:
            try:
                lbl.config(fg=fg_values, bg=bg_now)
            except:
                pass
        for lbl in self._detail_title_labels:
            try:
                lbl.config(fg=fg_titles, bg=bg_now)
            except:
                pass

    # ---- tags da Treeview (cores por linha) ----
    def _setup_tree_tags(self):
        if self.dark_var.get():
            self.tree.tag_configure("verde",    background="#13301a", foreground="#ffffff")
            self.tree.tag_configure("amarelo",  background="#2d2615", foreground="#ffffff")
            self.tree.tag_configure("vermelho", background="#2c1515", foreground="#ffffff")
        else:
            self.tree.tag_configure("verde",    background="#e6ffe6")
            self.tree.tag_configure("amarelo",  background="#fff7e6")
            self.tree.tag_configure("vermelho", background="#ffe6e6")

    # ---- construção de linhas ----
    def _mk_row(self, parent, titulo, key, row=0, bold=False, copy=False, copy_target=None):
        f = tk.Frame(parent, bg=self.detail_bg.get())
        f.grid(row=row, column=0, sticky="ew", padx=8, pady=(6 if row else 4))

        fg_val = "#ffffff" if self.dark_var.get() else "#111111"
        fg_tit = "#ffffff" if self.dark_var.get() else "#111111"

        lab_title = tk.Label(
            f, text=titulo, width=18, anchor="w",
            font=self.font_strong if bold else self.font_base,
            bg=self.detail_bg.get(), fg=fg_tit
        )
        lab_title.pack(side="left")

        lab_val = tk.Label(
            f, text="—", width=60, anchor="w",
            font=self.font_base, bg=self.detail_bg.get(), fg=fg_val
        )
        lab_val.pack(side="left", padx=(6, 6))
        setattr(self, f"lbl_{key}", lab_val)

        self._detail_title_labels.append(lab_title)
        self._detail_value_labels.append(lab_val)

        if copy:
            btn = ttk.Button(f, text="📋 Copiar", width=10)
            btn.pack(side="left")
            add_tooltip(btn, "Copiar para a área de transferência")
            if copy_target == "nome":
                btn.configure(command=partial(self._button_copy_nome, btn))
            elif copy_target == "cpf":
                btn.configure(command=partial(self._button_copy_cpf, btn))
            elif copy_target == "contrato":
                btn.configure(command=partial(self._button_copy_contrato, btn))

    # ---- status/busy ----
    def set_busy(self, flag=True, msg=None):
        self.config(cursor="watch" if flag else "")
        if msg is not None: self.status.config(text=msg)
        self.update_idletasks()

    # ---- carregar dados + conjuntos ----
    def _carregar_dados_e_conjuntos_async(self):
        in_list = ",".join(str(c) for c in self.carteiras)
        operador_where = ""

        if LOCKED_USER:
            op = LOCKED_USER.replace("'", "''").strip()
            operador_where = f"AND TRIM(usu.nomeusu) = '{op}'"
        elif self.operador:
            op = self.operador.replace("'", "''").strip()
            operador_where = f"AND TRIM(usu.nomeusu) = '{op}'"

        sql_main = SQL_BASE.format(
            in_list=in_list,
            operador_where=(" " + operador_where if operador_where else ""),
            extra_where=""
        )

        # QR / CPC / NAO
        sql_qr     = SQL_NMCONT_QR.format(in_list=in_list,  operador_where=(" " + operador_where if operador_where else ""))
        sql_cpc    = SQL_NMCONT_CPC.format(in_list=in_list, operador_where=(" " + operador_where if operador_where else ""))
        sql_nao    = SQL_NMCONT_NAO.format(in_list=in_list, operador_where=(" " + operador_where if operador_where else ""))
        # NOVO perfil
        sql_perfil = SQL_NMCONT_PERFIL.format(in_list=in_list, operador_where=(" " + operador_where if operador_where else ""))

        self.set_busy(True, "Carregando dados e filtros...")

        def job():
            try:
                conn = pymysql.connect(**DB)

                # base principal
                df_main = pd.read_sql_query(sql_main, conn)

                # conjuntos auxiliares
                try:
                    df_qr  = pd.read_sql_query(sql_qr,  conn)
                except Exception:
                    df_qr = pd.DataFrame(columns=["nmcont","data_aco","vlr_aco","qtd_p_aco"])

                try:
                    df_cpc = pd.read_sql_query(sql_cpc, conn)
                except Exception:
                    df_cpc = pd.DataFrame(columns=["nmcont","dt_ultimo_cpc"])

                try:
                    df_nao = pd.read_sql_query(sql_nao, conn)
                except Exception:
                    df_nao = pd.DataFrame(columns=["nmcont"])

                # perfil
                try:
                    df_perfil = pd.read_sql_query(sql_perfil, conn)
                except Exception:
                    df_perfil = pd.DataFrame(columns=[
                        "nmcont","infoad","comprometimento_credito","flag_aposentado",
                        "flag_bolsafamilia","flag_veiculo","flag_vinculo_empregaticio","flag_obito"
                    ])

                conn.close()

                # --- MESCLA: acordo ---
                if not df_qr.empty:
                    df_qr_ren = df_qr.rename(columns={"nmcont": "contrato"})
                    df_qr_ren["data_aco"] = pd.to_datetime(df_qr_ren["data_aco"], errors="coerce")

                    for col in ("data_aco","vlr_aco","qtd_p_aco","qtdaco"):
                        if col not in df_qr_ren.columns:
                            df_qr_ren[col] = pd.NA

                    df_main = df_main.merge(
                        df_qr_ren[["contrato","data_aco","vlr_aco","qtd_p_aco","qtdaco"]],
                        on="contrato", how="left"
                    )
                else:
                    df_main["data_aco"] = pd.NaT
                    df_main["vlr_aco"]  = pd.NA
                    df_main["qtd_p_aco"]= pd.NA
                    df_main["qtdaco"]   = pd.NA


                # --- MESCLA: CPC ---
                if not df_cpc.empty:
                    df_cpc_ren = df_cpc.rename(columns={"nmcont": "contrato"})
                    df_cpc_ren["dt_ultimo_cpc"] = pd.to_datetime(df_cpc_ren["dt_ultimo_cpc"], errors="coerce")
                    df_main = df_main.merge(
                        df_cpc_ren[["contrato", "dt_ultimo_cpc"]],
                        on="contrato", how="left"
                    )
                else:
                    df_main["dt_ultimo_cpc"] = pd.NaT

                # --- MESCLA: PERFIL ---
                if not df_perfil.empty:
                    df_pf = df_perfil.rename(columns={"nmcont":"contrato"}).copy()
                    # formatações amigáveis
                    df_pf["comprom_txt"] = df_pf["comprometimento_credito"].apply(_fmt_comprometimento)
                    df_pf["flag_apos_txt"]  = df_pf["flag_aposentado"].apply(_fmt_flag)
                    df_pf["flag_bolsa_txt"] = df_pf["flag_bolsafamilia"].apply(_fmt_flag)
                    df_pf["flag_veic_txt"]  = df_pf["flag_veiculo"].apply(_fmt_flag)
                    df_pf["flag_vinc_txt"]  = df_pf["flag_vinculo_empregaticio"].apply(_fmt_flag)
                    df_pf["flag_obito_txt"] = df_pf["flag_obito"].apply(_fmt_flag)

                    df_main = df_main.merge(
                        df_pf[["contrato","infoad","comprom_txt","flag_apos_txt","flag_bolsa_txt",
                               "flag_veic_txt","flag_vinc_txt","flag_obito_txt"]],
                        on="contrato", how="left"
                    )
                else:
                    for col in ("infoad","comprom_txt","flag_apos_txt","flag_bolsa_txt","flag_veic_txt","flag_vinc_txt","flag_obito_txt"):
                        df_main[col] = ""

                # conjuntos para filtros por nmcont
                set_qr  = set(df_qr["nmcont"].astype(str))  if not df_qr.empty  else set()
                set_cpc = set(df_cpc["nmcont"].astype(str)) if not df_cpc.empty else set()
                set_nao = set(df_nao["nmcont"].astype(str)) if not df_nao.empty else set()

                self.after(0, lambda: self._on_loaded_with_sets(df_main, set_qr, set_cpc, set_nao))
            except Exception as e:
                self.after(0, lambda: self._on_error(e))

        threading.Thread(target=job, daemon=True).start()

    def _on_error(self, e):
        self.set_busy(False, "Erro")
        messagebox.showerror("Erro", f"Falha ao consultar o banco:\n{e}")

    def _on_loaded_with_sets(self, df, set_qr, set_cpc, set_nao):
        # guarda conjuntos
        self.set_qr, self.set_cpc, self.set_nao = set_qr, set_cpc, set_nao
        self._atualizar_contadores_conjuntos()

        # guarda base completa
        self.df_all = df.copy()

        if self.df_all.empty:
            labels = [label for (label, code) in CARTEIRAS if code in self.carteiras]
            alvo = " (operador selecionado)" if self.operador else ""
            self.set_busy(False, "Sem registros")
            messagebox.showwarning("Aviso", f"Nenhum registro encontrado para {', '.join(labels)}{alvo}.")
            return

        # aplica (inicialmente sem filtros marcados)
        self._aplicar_filtros_nmcont(inicial=True)

        labels = [label for (label, code) in CARTEIRAS if code in self.carteiras]
        op_txt = "Todos" if not self.operador else self.operador
        self.lbl_ctx.config(text=f"Carteiras: {', '.join(labels)}  |  Operador: {op_txt}")
        self.set_busy(False, f"{len(self.df)} registros carregados.")

    def _atualizar_contadores_conjuntos(self):
        txt = f"(Q/R: {len(self.set_qr)} | CPC: {len(self.set_cpc)} | Não acion.: {len(self.set_nao)})"
        self.lbl_counts.config(text=txt)

    # ---- aplicar/limpar filtros nmcont + cor ----
    def _aplicar_filtros_nmcont(self, inicial=False):
        df_src = self.df_all.copy()
        if df_src.empty:
            self.df = df_src
            self._render_lista()
            return

        conjuntos = []
        if self.var_qr.get():  conjuntos.append(self.set_qr)
        if self.var_cpc.get(): conjuntos.append(self.set_cpc)
        if self.var_nao.get(): conjuntos.append(self.set_nao)

        if not conjuntos:
            df_filtrado = df_src
        else:
            allow = set().union(*conjuntos) if conjuntos else set()
            df_filtrado = df_src[df_src["contrato"].astype(str).isin(allow)]

        cor = (self.color_var.get() or "todos").lower()
        if cor != "todos":
            s = pd.to_datetime(df_filtrado["ultima_data"], errors="coerce")
            dias = (pd.Timestamp.today().normalize() - s).dt.days

            if cor == "verde":
                mask = (dias >= 0) & (dias <= 7)
            elif cor == "amarelo":
                mask = (dias >= 8) & (dias <= 30)
            elif cor == "vermelho":
                mask = (dias > 30)
            else:
                mask = pd.Series([True]*len(df_filtrado), index=df_filtrado.index)

            mask = mask & s.notna()
            df_filtrado = df_filtrado[mask]

        self.df = df_filtrado
        self.idx = 0
        self._render_lista()
        if not inicial:
            txt_cor = {"todos":"todos", "verde":"verdes", "amarelo":"amarelos", "vermelho":"vermelhos"}.get(cor, "todos")
            self.status.config(text=f"Filtros aplicados • {len(self.df)} registros • cor: {txt_cor}")

    def _limpar_filtros_nmcont(self):
        self.var_qr.set(False)
        self.var_cpc.set(False)
        self.var_nao.set(False)
        self.color_var.set("todos")
        self._aplicar_filtros_nmcont()
        self.status.config(text=f"Filtros limpos • {len(self.df)} registros")

    # ---- renderização ----
    def _render_lista(self):
        for i in self.tree.get_children():
            self.tree.delete(i)

        if self.df.empty:
            self._limpar_detalhe()
            return

        hoje_norm = pd.Timestamp.today().normalize()

        for _, r in self.df.iterrows():
            contrato = str(r["contrato"])
            nome     = str(r["nomecli"])
            cpf_fmt  = fmt_cpf_cnpj(r["cpfcnpj"])
            usuario  = str(r["nomeusu"])

            s_data = pd.to_datetime(r["ultima_data"], errors="coerce")
            dt = str(s_data.date()) if pd.notna(s_data) else ""

            tags = ()
            if pd.notna(s_data):
                try:
                    dias = (hoje_norm - s_data.normalize()).days
                    if dias <= 7:
                        tags = ("verde",)
                    elif dias <= 30:
                        tags = ("amarelo",)
                    else:
                        tags = ("vermelho",)
                except Exception:
                    pass

            s_aco = pd.to_datetime(r.get("data_aco"), errors="coerce")
            aco_dt = str(s_aco.date()) if pd.notna(s_aco) else ""

            val = r.get("vlr_aco")
            if pd.isna(val):
                aco_vlr = ""
            else:
                try:
                    aco_vlr = f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                except Exception:
                    aco_vlr = str(val)

            qtd = r.get("qtd_p_aco")
            if pd.isna(qtd):
                aco_qtd = ""
            else:
                try:
                    aco_qtd = str(int(qtd))
                except Exception:
                    aco_qtd = str(qtd)

            self.tree.insert(
                "", "end",
                values=(contrato, nome, cpf_fmt, usuario, dt, aco_dt, aco_vlr, aco_qtd),
                tags=tags
            )

        self.idx = 0
        self._mostrar_atual()
        self._atualizar_botoes()
        self.nb.select(self.tab_detalhe)

    def _limpar_detalhe(self):
        bg = self.detail_bg.get()
        try:
            self.lbl_contrato.config(text="—", bg=bg)
            self.lbl_usuario.config(text="—", bg=bg)
            self.lbl_nome.config(text="—", bg=bg)
            self.lbl_cpf.config(text="—", bg=bg)
            self.lbl_data.config(text="—", bg=bg)
            self.lbl_aco_data.config(text="—", bg=bg)
            self.lbl_aco_valor.config(text="—", bg=bg)
            self.lbl_aco_qtd.config(text="—", bg=bg)
            self.lbl_cpc_data.config(text="—", bg=bg)
            self.lbl_infoad.config(text="—", bg=bg)
            self.lbl_comprom.config(text="—", bg=bg)
            self.lbl_flag_apos.config(text="—", bg=bg)
            self.lbl_flag_bolsa.config(text="—", bg=bg)
            self.lbl_flag_veic.config(text="—", bg=bg)
            self.lbl_flag_vinc.config(text="—", bg=bg)
            self.lbl_flag_obito.config(text="—", bg=bg)
            self.lbl_qtdaco.config(text="—", bg=bg)
        except Exception:
            pass

        self.btn_prev.config(state="disabled")
        self.btn_next.config(state="disabled")

    def _mostrar_atual(self):
        row = self.df.iloc[self.idx]

        bg = cor_por_data(row["ultima_data"], dark=self.dark_var.get()) if pd.notna(row["ultima_data"]) else (
            "#ffffff" if not self.dark_var.get() else "#0f1115"
        )
        self.detail_bg.set(bg)
        self._update_detail_bgs(bg)

        contrato = str(row["contrato"])
        nome = str(row["nomecli"])
        cpf_fmt = fmt_cpf_cnpj(row["cpfcnpj"])
        usuario = str(row["nomeusu"])

        try:
            dt = pd.to_datetime(row["ultima_data"]).strftime("%d/%m/%Y")
        except Exception:
            dt = "" if pd.isna(row["ultima_data"]) else str(row["ultima_data"])

        try:
            aco_dt = pd.to_datetime(row.get("data_aco")).strftime("%d/%m/%Y")
        except Exception:
            aco_dt = "" if pd.isna(row.get("data_aco")) else str(row.get("data_aco"))

        val = row.get("vlr_aco")
        if pd.isna(val):
            aco_val = ""
        else:
            try:
                aco_val = f"R$ {float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            except Exception:
                aco_val = str(val)

        qtd = row.get("qtd_p_aco")
        if pd.isna(qtd):
            aco_qtd = ""
        else:
            try:
                aco_qtd = str(int(qtd))
            except Exception:
                aco_qtd = str(qtd)

        # dt_ultimo_cpc -> label
        try:
            cpc_dt = pd.to_datetime(row.get("dt_ultimo_cpc")).strftime("%d/%m/%Y")
        except Exception:
            cpc_dt = "" if pd.isna(row.get("dt_ultimo_cpc")) else str(row.get("dt_ultimo_cpc"))

        # Perfil
        infoad = str(row.get("infoad") or "").strip()
        comprom = str(row.get("comprom_txt") or "")
        f_apos  = str(row.get("flag_apos_txt") or "—")
        f_bolsa = str(row.get("flag_bolsa_txt") or "—")
        f_veic  = str(row.get("flag_veic_txt") or "—")
        f_vinc  = str(row.get("flag_vinc_txt") or "—")
        f_obito = str(row.get("flag_obito_txt") or "—")

        self.lbl_contrato.config(text=contrato, bg=bg)
        self.lbl_usuario.config(text=usuario, bg=bg)
        self.lbl_nome.config(text=nome, bg=bg)
        self.lbl_cpf.config(text=cpf_fmt, bg=bg)
        self.lbl_data.config(text=dt, bg=bg)

        self.lbl_aco_data.config(text=aco_dt, bg=bg)
        self.lbl_aco_valor.config(text=aco_val, bg=bg)
        self.lbl_aco_qtd.config(text=aco_qtd, bg=bg)
        self.lbl_cpc_data.config(text=cpc_dt, bg=bg)
        # qtdaco (quantidade de propostas formalizadas)
        qtdaco_val = row.get("qtdaco")
        if pd.isna(qtdaco_val) or qtdaco_val is None or str(qtdaco_val).strip() == "":
            qtdaco_txt = "—"
        else:
            try:
                qtdaco_txt = str(int(float(qtdaco_val)))
            except Exception:
                qtdaco_txt = str(qtdaco_val)

        self.lbl_qtdaco.config(text=qtdaco_txt, bg=bg)


        # perfil
        self.lbl_infoad.config(text=infoad or "—", bg=bg)
        self.lbl_comprom.config(text=comprom or "—", bg=bg)
        self.lbl_flag_apos.config(text=f_apos, bg=bg)
        self.lbl_flag_bolsa.config(text=f_bolsa, bg=bg)
        self.lbl_flag_veic.config(text=f_veic, bg=bg)
        self.lbl_flag_vinc.config(text=f_vinc, bg=bg)
        self.lbl_flag_obito.config(text=f_obito, bg=bg)

        self._refresh_detail_colors()

    # ---- navegação ----
    def _atualizar_botoes(self):
        self.btn_prev.config(state=("normal" if self.idx > 0 else "disabled"))
        self.btn_next.config(state=("normal" if self.idx < len(self.df) - 1 else "disabled"))
        self.idx_var.set(self.idx + 1)

    def anterior(self):
        if self.idx > 0:
            self.idx -= 1
            self._mostrar_atual(); self._atualizar_botoes()

    def proximo(self):
        if self.idx < len(self.df) - 1:
            self.idx += 1
            self._mostrar_atual(); self._atualizar_botoes()

    def _goto(self, i):
        if 0 <= i < len(self.df):
            self.idx = i
            self._mostrar_atual(); self._atualizar_botoes()
            self.nb.select(self.tab_detalhe)

    def _ir_para_detalhe_por_duplo_clique(self, event):
        item = self.tree.focus()
        if not item: return
        vals = self.tree.item(item, "values")
        if not vals: return
        contrato = vals[0]
        try:
            pos = self.df.index[self.df["contrato"].astype(str) == str(contrato)][0]
            self._goto(int(pos))
        except Exception:
            pass

    # ---- copiar ----
    def _copy_to_clipboard(self, texto, btn=None):
        try:
            self.clipboard_clear(); self.clipboard_append(texto); self.update()
            if btn: flash_button(btn)
        except Exception as e:
            messagebox.showwarning("Copiar", f"Não foi possível copiar:\n{e}")

    def _copy_current_nome(self, btn=None):
        row = self.df.iloc[self.idx]
        self._copy_to_clipboard(str(row["nomecli"]), btn)

    def _copy_current_cpf(self, btn=None):
        row = self.df.iloc[self.idx]
        self._copy_to_clipboard(only_digits(row["cpfcnpj"]), btn)

    def _button_copy_nome(self, button_widget):
        self._copy_current_nome(button_widget)

    def _button_copy_cpf(self, button_widget):
        self._copy_current_cpf(button_widget)

    def _button_copy_contrato(self, button_widget):
        row = self.df.iloc[self.idx]
        self._copy_to_clipboard(str(row["contrato"]), button_widget)

    # ---- exportar ----
    def _df_export_base(self, df_src):
        df_exp = df_src.copy()
        df_exp["CPFCNPJ_Limpo"] = df_exp["cpfcnpj"].apply(only_digits)
        df_exp["CPFCNPJ_Formatado"] = df_src["cpfcnpj"].apply(fmt_cpf_cnpj)
        df_exp["ultima_data"] = pd.to_datetime(df_exp["ultima_data"]).dt.date.astype(str)
        cols = ["contrato","nomecli","CPFCNPJ_Limpo","CPFCNPJ_Formatado","nomeusu","ultima_data"]
        return df_exp[cols]

    def exportar_csv_tudo(self):
        if self.df.empty:
            messagebox.showinfo("Exportar CSV", "Não há registros para exportar.")
            return
        df_exp = self._df_export_base(self.df)
        path = filedialog.asksaveasfilename(
            title="Salvar CSV (Tudo)",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"lista_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        if not path: return
        try:
            df_exp.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
            messagebox.showinfo("Exportar CSV", f"Arquivo salvo em:\n{path}")
        except Exception as e:
            messagebox.showerror("Exportar CSV", f"Falha ao salvar:\n{e}")

    def exportar_csv_selecao(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Exportar Seleção", "Selecione uma ou mais linhas na aba Lista.")
            self.nb.select(self.tab_lista)
            return
        contratos = [self.tree.item(i, "values")[0] for i in sel]
        df_sel = self.df[self.df["contrato"].astype(str).isin([str(c) for c in contratos])]
        if df_sel.empty:
            messagebox.showinfo("Exportar Seleção", "Seleção vazia.")
            return
        df_exp = self._df_export_base(df_sel)
        path = filedialog.asksaveasfilename(
            title="Salvar CSV (Seleção)",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            initialfile=f"selecao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        if not path: return
        try:
            df_exp.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
            messagebox.showinfo("Exportar CSV", f"Arquivo salvo em:\n{path}")
        except Exception as e:
            messagebox.showerror("Exportar CSV", f"Falha ao salvar:\n{e}")

    def copiar_detalhe(self):
        if self.df.empty: return
        r = self.df.iloc[self.idx]
        linha = ";".join([
            str(r["contrato"]),
            str(r["nomecli"]).replace(";", ","),
            only_digits(r["cpfcnpj"]),
            fmt_cpf_cnpj(r["cpfcnpj"]),
            str(r["nomeusu"]).replace(";", ","),
            str(pd.to_datetime(r["ultima_data"]).date()) if pd.notna(r["ultima_data"]) else ""
        ])
        self._copy_to_clipboard(linha)

    def _toggle_dark(self):
        apply_palette(self, self.dark_var.get())
        bg_now = self.detail_bg.get()
        self._update_detail_bgs(bg_now)
        self._refresh_detail_colors()
        self._setup_tree_tags()
        if not self.df.empty:
            self._mostrar_atual()

    # ---- voltar ----
    def voltar_inicio(self):
        self._restart = True
        self.destroy()

    @property
    def restart(self):
        return self._restart


def rodar_fluxo():
    while True:
        seletor = TelaInicial()
        seletor.mainloop()
        carteiras, operador = seletor.resultado
        if not carteiras:
            break
        app = TelaDados(carteiras, operador)
        app.mainloop()
        if not app.restart:
            break

if __name__ == "__main__":
    rodar_fluxo()
