import threading
from sqlalchemy import text
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    login_required,
    logout_user,
    current_user,
)
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask import Flask, render_template, request, redirect, send_file, flash, url_for
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import os
import re
import json
from datetime import datetime
from io import BytesIO
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)

secret_key = os.environ.get("SECRET_KEY")
if not secret_key:
    secret_key = os.urandom(32).hex()
    print("⚠️ SECRET_KEY não encontrada. Usando chave aleatória local temporária.")

app.config["SECRET_KEY"] = secret_key

database_url = os.environ.get("DATABASE_URL")
if not database_url:
    database_url = f"sqlite:///{os.path.join(BASE_DIR, 'local.db')}"
    print("⚠️ DATABASE_URL não encontrada. Usando SQLite local em local.db.")

if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True,
}

serializer = URLSafeTimedSerializer(app.config["SECRET_KEY"])

db = SQLAlchemy(app)
migrate = Migrate(app, db)


login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message = "Faça login para acessar o sistema."
login_manager.login_message_category = "warning"

# =========================================================
# MODELOS
# =========================================================
class Usuario(UserMixin, db.Model):
    __tablename__ = "usuarios"

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(150), nullable=False)
    email = db.Column(db.String(150), unique=True, nullable=False, index=True)
    senha_hash = db.Column(db.String(255), nullable=False)
    ativo = db.Column(db.Boolean, default=True, nullable=False)
    criado_em = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    registros = db.relationship(
        "Empresa",
        back_populates="usuario",
        lazy=True,
        cascade="all, delete-orphan",
    )

    def set_senha(self, senha):
        self.senha_hash = generate_password_hash(senha)

    def check_senha(self, senha):
        return check_password_hash(self.senha_hash, senha)


class Empresa(db.Model):
    __tablename__ = "empresas"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False, index=True)

    nome = db.Column(db.String(150), nullable=False)
    cpf_cnpj = db.Column(db.String(20), nullable=False, index=True)
    celular = db.Column(db.String(20))
    uf = db.Column(db.String(2))
    cidade = db.Column(db.String(100))
    bairro = db.Column(db.String(100))
    cep = db.Column(db.String(20))
    endereco = db.Column(db.String(150))
    numero = db.Column(db.String(20))
    criado_em = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    atualizado_em = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    usuario = db.relationship("Usuario", back_populates="registros")


@login_manager.user_loader
def load_user(user_id):
    try:
        return db.session.get(Usuario, int(user_id))
    except (TypeError, ValueError):
        return None


# =========================================================
# CONSTANTES
# =========================================================
ARQUIVO_AUDITORIA = os.path.join(BASE_DIR, "auditoria.jsonl")

COLUNAS = [
    "ID",
    "Nome",
    "CPF/CNPJ",
    "Celular",
    "UF",
    "Cidade",
    "Bairro",
    "CEP",
    "Endereço",
    "Número",
]

ALIAS_COLUNAS = {
    "id": "ID",
    "codigo": "ID",
    "código": "ID",
    "nome": "Nome",
    "razao social": "Nome",
    "razão social": "Nome",
    "empresa": "Nome",
    "pessoa": "Nome",
    "cliente": "Nome",
    "cpf/cnpj": "CPF/CNPJ",
    "cpf_cnpj": "CPF/CNPJ",
    "cpfcnpj": "CPF/CNPJ",
    "documento": "CPF/CNPJ",
    "cnpj": "CPF/CNPJ",
    "cpf": "CPF/CNPJ",
    "celular": "Celular",
    "telefone": "Celular",
    "fone": "Celular",
    "whatsapp": "Celular",
    "uf": "UF",
    "estado": "UF",
    "cidade": "Cidade",
    "municipio": "Cidade",
    "município": "Cidade",
    "bairro": "Bairro",
    "cep": "CEP",
    "endereco": "Endereço",
    "endereço": "Endereço",
    "logradouro": "Endereço",
    "rua": "Endereço",
    "numero": "Número",
    "número": "Número",
    "num": "Número",
    "n": "Número",
}

# =========================================================
# UTILITÁRIOS BÁSICOS
# =========================================================
def apenas_numeros(valor):
    return re.sub(r"\D", "", str(valor or ""))


def limpar_texto(valor):
    if valor is None:
        return ""
    return str(valor).strip()


def limpar_texto_titulo(valor):
    return limpar_texto(valor)


def normalizar_chave_coluna(nome):
    nome = limpar_texto(nome).lower()
    nome = re.sub(r"\s+", " ", nome)
    return nome


def timestamp_agora():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def registrar_auditoria(acao, detalhes=None):
    payload = {
        "data_hora": timestamp_agora(),
        "acao": acao,
        "detalhes": detalhes or {},
    }
    try:
        with open(ARQUIVO_AUDITORIA, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        app.logger.exception("Erro ao registrar auditoria")


def criar_admin_padrao():
    """
    Em produção: crie ADMIN_EMAIL e ADMIN_PASSWORD no ambiente.
    Localmente, se não existir nenhum usuário, cria um admin básico para teste.
    """
    usuario_existente = Usuario.query.first()
    if usuario_existente:
        return

    email_admin = os.environ.get("ADMIN_EMAIL")
    senha_admin = os.environ.get("ADMIN_PASSWORD")

    if not email_admin or not senha_admin:
        if app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite"):
            email_admin = "admin@admin.com"
            senha_admin = "123456"
            print("⚠️ Admin local criado com credenciais padrão para ambiente de teste.")
        else:
            print("⚠️ ADMIN_EMAIL / ADMIN_PASSWORD não configurados. Admin padrão não será criado.")
            return

    usuario = Usuario(
        nome="Administrador",
        email=email_admin,
        ativo=True,
    )
    usuario.set_senha(senha_admin)

    db.session.add(usuario)
    db.session.commit()

    print("✅ Admin criado com sucesso")
    print("Login:", email_admin)
    print("Senha:", senha_admin)


def resetar_sequence_empresas_do_usuario_se_vazio(user_id):
    total = Empresa.query.filter_by(user_id=user_id).count()
    if total != 0:
        return

    uri = app.config["SQLALCHEMY_DATABASE_URI"]
    if uri.startswith("postgresql"):
        try:
            db.session.execute(
                text(
                    "SELECT setval("
                    "pg_get_serial_sequence('empresas', 'id'), "
                    "COALESCE((SELECT MAX(id) FROM empresas), 1), true)"
                )
            )
            db.session.commit()
        except Exception:
            db.session.rollback()
            app.logger.exception("Erro ao resetar sequence do PostgreSQL")

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def enviar_email_recuperacao(destinatario, link):
    email_remetente = os.getenv("EMAIL_FROM")
    senha_app = os.getenv("EMAIL_APP_PASSWORD")

    if not email_remetente or not senha_app:
        raise ValueError("EMAIL_FROM ou EMAIL_APP_PASSWORD não configurados.")

    assunto = "Recuperação de senha"
    corpo_html = f"""
    <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <h2>Recuperação de senha</h2>
            <p>Recebemos uma solicitação para redefinir sua senha.</p>
            <p>Clique no botão abaixo para criar uma nova senha:</p>
            <p>
                <a href="{link}" style="
                    display:inline-block;
                    padding:12px 20px;
                    background:#0d6efd;
                    color:#fff;
                    text-decoration:none;
                    border-radius:8px;
                    font-weight:bold;
                ">
                    Redefinir senha
                </a>
            </p>
            <p>Se você não fez esta solicitação, ignore este e-mail.</p>
        </body>
    </html>
    """

    msg = MIMEMultipart()
    msg["From"] = email_remetente
    msg["To"] = destinatario
    msg["Subject"] = assunto
    msg.attach(MIMEText(corpo_html, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as servidor:
        servidor.login(email_remetente, senha_app)
        servidor.send_message(msg)



# =========================================================
# FORMATAÇÃO
# =========================================================
def formatar_cep(cep):
    cep = apenas_numeros(cep)[:8]
    if len(cep) == 8:
        return f"{cep[:5]}-{cep[5:]}"
    return cep


def formatar_celular(celular):
    celular = apenas_numeros(celular)[:11]

    if len(celular) == 11:
        return f"({celular[:2]}) {celular[2:7]}-{celular[7:]}"
    if len(celular) == 10:
        return f"({celular[:2]}) {celular[2:6]}-{celular[6:]}"
    return celular


def formatar_cpf(cpf):
    cpf = apenas_numeros(cpf)[:11]
    if len(cpf) == 11:
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
    return cpf


def formatar_cnpj(cnpj):
    cnpj = apenas_numeros(cnpj)[:14]
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    return cnpj


def formatar_documento(doc):
    numeros = apenas_numeros(doc)
    if len(numeros) == 11:
        return formatar_cpf(numeros)
    if len(numeros) == 14:
        return formatar_cnpj(numeros)
    return limpar_texto(doc)


def padronizar_uf(uf):
    return limpar_texto(uf).upper()[:2]


def padronizar_nome(nome):
    return limpar_texto_titulo(nome)


def padronizar_cidade(cidade):
    return limpar_texto_titulo(cidade)


def padronizar_bairro(bairro):
    return limpar_texto_titulo(bairro)


def padronizar_endereco(endereco):
    return limpar_texto_titulo(endereco)


# =========================================================
# VALIDAÇÕES
# =========================================================
def validar_cpf(cpf):
    cpf = apenas_numeros(cpf)

    if len(cpf) != 11:
        return False
    if cpf == cpf[0] * 11:
        return False

    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    digito_1 = (soma * 10) % 11
    digito_1 = 0 if digito_1 == 10 else digito_1

    if digito_1 != int(cpf[9]):
        return False

    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    digito_2 = (soma * 10) % 11
    digito_2 = 0 if digito_2 == 10 else digito_2

    return digito_2 == int(cpf[10])


def validar_cnpj(cnpj):
    cnpj = apenas_numeros(cnpj)

    if len(cnpj) != 14:
        return False
    if cnpj == cnpj[0] * 14:
        return False

    pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos_2 = [6] + pesos_1

    soma_1 = sum(int(cnpj[i]) * pesos_1[i] for i in range(12))
    resto_1 = soma_1 % 11
    digito_1 = 0 if resto_1 < 2 else 11 - resto_1

    if digito_1 != int(cnpj[12]):
        return False

    soma_2 = sum(int(cnpj[i]) * pesos_2[i] for i in range(13))
    resto_2 = soma_2 % 11
    digito_2 = 0 if resto_2 < 2 else 11 - resto_2

    return digito_2 == int(cnpj[13])


def validar_documento(documento):
    doc = apenas_numeros(documento)
    if len(doc) == 11:
        return validar_cpf(doc)
    if len(doc) == 14:
        return validar_cnpj(doc)
    return False


def tipo_documento(documento):
    doc = apenas_numeros(documento)
    if len(doc) == 11:
        return "pessoa"
    if len(doc) == 14:
        return "empresa"
    return "desconhecido"


def validar_campos_obrigatorios(nome, documento):
    erros = []

    if not limpar_texto(nome):
        erros.append("O campo Nome é obrigatório.")

    if not limpar_texto(documento):
        erros.append("O campo CPF/CNPJ é obrigatório.")
    elif not validar_documento(documento):
        erros.append("CPF/CNPJ inválido.")

    return erros


# =========================================================
# FUNÇÕES DE BANCO
# =========================================================
def empresa_para_dict(empresa):
    return {
        "ID": str(empresa.id),
        "Nome": empresa.nome or "",
        "CPF/CNPJ": formatar_documento(empresa.cpf_cnpj),
        "Celular": formatar_celular(empresa.celular),
        "UF": empresa.uf or "",
        "Cidade": empresa.cidade or "",
        "Bairro": empresa.bairro or "",
        "CEP": formatar_cep(empresa.cep),
        "Endereço": empresa.endereco or "",
        "Número": empresa.numero or "",
    }


def listar_empresas_db(user_id):
    empresas = (
        Empresa.query
        .filter_by(user_id=user_id)
        .order_by(Empresa.id.desc())
        .all()
    )
    dados = [empresa_para_dict(emp) for emp in empresas]
    return pd.DataFrame(dados, columns=COLUNAS)


def buscar_empresa_db(id_registro, user_id):
    return Empresa.query.filter_by(id=id_registro, user_id=user_id).first()


def documento_ja_existe_db(documento, user_id, ignorar_id=None):
    doc = formatar_documento(documento)

    query = Empresa.query.filter_by(cpf_cnpj=doc, user_id=user_id)
    if ignorar_id is not None:
        query = query.filter(Empresa.id != ignorar_id)

    return db.session.query(query.exists()).scalar()


# =========================================================
# EXCEL
# =========================================================
def normalizar_dataframe(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=COLUNAS)

    df = df.copy()

    for coluna in list(df.columns):
        coluna_normalizada = normalizar_chave_coluna(coluna)
        if coluna_normalizada in ALIAS_COLUNAS:
            df = df.rename(columns={coluna: ALIAS_COLUNAS[coluna_normalizada]})

    for coluna in COLUNAS:
        if coluna not in df.columns:
            df[coluna] = ""

    df = df[COLUNAS].fillna("")

    for coluna in COLUNAS:
        df[coluna] = df[coluna].astype(str).fillna("").map(limpar_texto)

    if "ID" in df.columns:
        df["ID"] = df["ID"].replace({"nan": "", "None": ""})

    df["Nome"] = df["Nome"].map(padronizar_nome)
    df["CPF/CNPJ"] = df["CPF/CNPJ"].map(formatar_documento)
    df["Celular"] = df["Celular"].map(formatar_celular)
    df["UF"] = df["UF"].map(padronizar_uf)
    df["Cidade"] = df["Cidade"].map(padronizar_cidade)
    df["Bairro"] = df["Bairro"].map(padronizar_bairro)
    df["CEP"] = df["CEP"].map(formatar_cep)
    df["Endereço"] = df["Endereço"].map(padronizar_endereco)
    df["Número"] = df["Número"].map(limpar_texto)

    return df


def valor_coluna(linha, coluna):
    if coluna is None:
        return ""
    return linha.get(coluna, "")


def importar_dataframe(df_origem, df_atual):
    df_origem = normalizar_dataframe(df_origem)

    importados = []
    ignorados = 0

    docs_existentes = (
        set(df_atual["CPF/CNPJ"].astype(str).apply(apenas_numeros).tolist())
        if not df_atual.empty else set()
    )

    for _, linha in df_origem.iterrows():
        nome = padronizar_nome(valor_coluna(linha, "Nome"))
        documento = formatar_documento(valor_coluna(linha, "CPF/CNPJ"))
        celular = formatar_celular(valor_coluna(linha, "Celular"))
        uf = padronizar_uf(valor_coluna(linha, "UF"))
        cidade = padronizar_cidade(valor_coluna(linha, "Cidade"))
        bairro = padronizar_bairro(valor_coluna(linha, "Bairro"))
        cep = formatar_cep(valor_coluna(linha, "CEP"))
        endereco = padronizar_endereco(valor_coluna(linha, "Endereço"))
        numero = limpar_texto(valor_coluna(linha, "Número"))

        if not nome or not documento:
            ignorados += 1
            continue

        doc_limpo = apenas_numeros(documento)

        if not validar_documento(documento):
            ignorados += 1
            continue

        if doc_limpo in docs_existentes:
            ignorados += 1
            continue

        registro = {
            "Nome": nome[:150],
            "CPF/CNPJ": documento[:20],
            "Celular": celular[:20],
            "UF": uf[:2],
            "Cidade": cidade[:100],
            "Bairro": bairro[:100],
            "CEP": cep[:20],
            "Endereço": endereco[:150],
            "Número": numero[:20],
        }

        importados.append(registro)
        docs_existentes.add(doc_limpo)

    return importados, ignorados


# =========================================================
# FILTRO / ORDENAÇÃO / RESUMO / PAGINAÇÃO
# =========================================================
def aplicar_filtros(df, busca="", tipo=""):
    df_filtrado = df.copy()

    busca = limpar_texto(busca)
    tipo = limpar_texto(tipo).lower()

    if busca:
        termo = busca.lower()

        mascara = (
            df_filtrado["Nome"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["CPF/CNPJ"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["Celular"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["UF"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["Cidade"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["Bairro"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["CEP"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["Endereço"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["Número"].astype(str).str.lower().str.contains(termo, na=False)
            | df_filtrado["ID"].astype(str).str.lower().str.contains(termo, na=False)
        )
        df_filtrado = df_filtrado[mascara]

    if tipo == "pessoa":
        df_filtrado = df_filtrado[
            df_filtrado["CPF/CNPJ"].astype(str).apply(lambda x: len(apenas_numeros(x)) == 11)
        ]
    elif tipo == "empresa":
        df_filtrado = df_filtrado[
            df_filtrado["CPF/CNPJ"].astype(str).apply(lambda x: len(apenas_numeros(x)) == 14)
        ]

    return df_filtrado


def aplicar_ordenacao(df, ordem="id_desc"):
    df_ordenado = df.copy()
    ordem = limpar_texto(ordem) or "id_desc"

    if ordem in ("id_asc", "id_desc"):
        df_ordenado["_id_num"] = pd.to_numeric(df_ordenado["ID"], errors="coerce")
        df_ordenado = df_ordenado.sort_values(
            by="_id_num",
            ascending=(ordem == "id_asc"),
            na_position="last",
            kind="stable",
        )
        return df_ordenado.drop(columns=["_id_num"])

    if ordem == "nome_asc":
        return df_ordenado.sort_values(by="Nome", ascending=True, na_position="last", kind="stable")

    if ordem == "nome_desc":
        return df_ordenado.sort_values(by="Nome", ascending=False, na_position="last", kind="stable")

    if ordem == "cidade_asc":
        return df_ordenado.sort_values(by="Cidade", ascending=True, na_position="last", kind="stable")

    if ordem == "cidade_desc":
        return df_ordenado.sort_values(by="Cidade", ascending=False, na_position="last", kind="stable")

    df_ordenado["_id_num"] = pd.to_numeric(df_ordenado["ID"], errors="coerce")
    df_ordenado = df_ordenado.sort_values(by="_id_num", ascending=False, na_position="last", kind="stable")
    return df_ordenado.drop(columns=["_id_num"])


def gerar_resumo(df_total, df_filtrado):
    documentos = df_total["CPF/CNPJ"].astype(str).apply(apenas_numeros) if not df_total.empty else pd.Series(dtype=str)

    total_pessoas = int((documentos.str.len() == 11).sum()) if not df_total.empty else 0
    total_empresas = int((documentos.str.len() == 14).sum()) if not df_total.empty else 0

    return {
        "total_registros": int(len(df_total)),
        "total_filtrado": int(len(df_filtrado)),
        "total_pessoas": total_pessoas,
        "total_empresas": total_empresas,
    }


def paginar_dataframe(df, pagina=1, por_pagina=10):
    total_resultados = int(len(df))
    total_paginas = max(1, (total_resultados + por_pagina - 1) // por_pagina)

    if pagina < 1:
        pagina = 1
    if pagina > total_paginas:
        pagina = total_paginas

    inicio = (pagina - 1) * por_pagina
    fim = inicio + por_pagina

    df_pagina = df.iloc[inicio:fim].copy()
    return {
        "dados": df_pagina.to_dict(orient="records"),
        "pagina": pagina,
        "total_paginas": total_paginas,
        "total_resultados": total_resultados,
        "inicio": inicio,
        "fim": min(fim, total_resultados),
    }


# =========================================================
# PREPARAÇÃO DE REGISTRO
# =========================================================
def montar_registro_form(form, id_existente=None):
    return {
        "ID": str(id_existente) if id_existente is not None else "",
        "Nome": padronizar_nome(form.get("nome")),
        "CPF/CNPJ": formatar_documento(form.get("cpf_cnpj")),
        "Celular": formatar_celular(form.get("celular")),
        "UF": padronizar_uf(form.get("uf")),
        "Cidade": padronizar_cidade(form.get("cidade")),
        "Bairro": padronizar_bairro(form.get("bairro")),
        "CEP": formatar_cep(form.get("cep")),
        "Endereço": padronizar_endereco(form.get("endereco")),
        "Número": limpar_texto(form.get("numero")),
    }


def calcular_completude(registro):
    campos = ["Nome", "CPF/CNPJ", "Celular", "UF", "Cidade", "Bairro", "CEP", "Endereço", "Número"]
    preenchidos = sum(1 for campo in campos if limpar_texto(registro.get(campo)))
    return int((preenchidos / len(campos)) * 100) if campos else 0


# =========================================================
# ROTAS DE AUTENTICAÇÃO
# =========================================================
@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        email = limpar_texto(request.form.get("email")).lower()
        senha = request.form.get("senha", "")

        usuario = Usuario.query.filter_by(email=email, ativo=True).first()

        if not usuario or not usuario.check_senha(senha):
            flash("E-mail ou senha inválidos.", "danger")
            return render_template("login.html")

        login_user(usuario)
        flash(f"Bem-vindo, {usuario.nome}!", "success")
        return redirect(url_for("index"))

    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    if request.method == "POST":
        nome = limpar_texto(request.form.get("nome"))
        email = limpar_texto(request.form.get("email")).lower()
        senha = request.form.get("senha", "")
        confirmar_senha = request.form.get("confirmar_senha", "")

        if not nome:
            flash("Informe o nome.", "warning")
            return redirect(url_for("register"))

        if not email:
            flash("Informe o e-mail.", "warning")
            return redirect(url_for("register"))

        if not senha:
            flash("Informe a senha.", "warning")
            return redirect(url_for("register"))

        if len(senha) < 6:
            flash("A senha deve ter pelo menos 6 caracteres.", "warning")
            return redirect(url_for("register"))

        if senha != confirmar_senha:
            flash("As senhas não coincidem.", "warning")
            return redirect(url_for("register"))

        usuario_existente = Usuario.query.filter_by(email=email).first()
        if usuario_existente:
            flash("Este e-mail já está cadastrado.", "danger")
            return redirect(url_for("register"))

        try:
            novo_usuario = Usuario(nome=nome, email=email, ativo=True)
            novo_usuario.set_senha(senha)

            db.session.add(novo_usuario)
            db.session.commit()

            flash("Cadastro realizado com sucesso. Agora faça login.", "success")
            return redirect(url_for("login"))

        except Exception:
            db.session.rollback()
            app.logger.exception("Erro ao cadastrar usuário")
            flash("Erro interno ao cadastrar usuário.", "danger")
            return redirect(url_for("register"))

    return render_template("register.html")


@app.route("/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    flash("Você saiu do sistema com sucesso.", "success")
    return redirect(url_for("login"))


@app.route("/usuarios")
@login_required
def listar_usuarios():
    usuarios = [current_user]
    return render_template("usuarios.html", usuarios=usuarios)

import traceback


def gerar_token_recuperacao(email):
    serializer = URLSafeTimedSerializer(app.secret_key)
    return serializer.dumps(email, salt="recuperacao-senha")


def validar_token_recuperacao(token, expiracao=1800):
    serializer = URLSafeTimedSerializer(app.secret_key)
    try:
        return serializer.loads(token, salt="recuperacao-senha", max_age=expiracao)
    except:
        return None

import threading
from flask import request, render_template, redirect, url_for, flash

@app.route("/esqueci-senha", methods=["GET", "POST"])
def esqueci_senha():
    if request.method == "POST":
        try:
            email = request.form.get("email", "").strip()

            if not email:
                flash("Informe um e-mail válido.", "warning")
                return redirect(url_for("esqueci_senha"))

            usuario = Usuario.query.filter_by(email=email).first()

            if usuario:
                # 🔐 Gera token
                token = gerar_token_recuperacao(usuario.email)

                # 🔗 Gera link correto (IMPORTANTE usar _external=True)
                link = url_for("redefinir_senha", token=token, _external=True)

                # 🚀 ENVIO EM SEGUNDO PLANO (resolve o erro do Render)
                threading.Thread(
                    target=enviar_email_recuperacao,
                    args=(usuario.email, link),
                    daemon=True
                ).start()

            # 🔒 Segurança: nunca fala se o e-mail existe ou não
            flash("Se o e-mail estiver cadastrado, você receberá um link de recuperação.", "success")
            return redirect(url_for("login"))

        except Exception as e:
            print("ERRO NA RECUPERAÇÃO DE SENHA:", str(e))
            import traceback
            traceback.print_exc()

            flash("Erro ao processar a recuperação de senha.", "danger")
            return redirect(url_for("esqueci_senha"))

    return render_template("esqueci_senha.html")

@app.route("/redefinir-senha/<token>", methods=["GET", "POST"])
def redefinir_senha(token):
    email = validar_token_recuperacao(token)

    if not email:
        flash("Link inválido ou expirado.", "danger")
        return redirect(url_for("login"))

    usuario = Usuario.query.filter_by(email=email).first()

    if not usuario:
        flash("Usuário não encontrado.", "danger")
        return redirect(url_for("login"))

    if request.method == "POST":
        senha = request.form.get("senha", "").strip()
        confirmar = request.form.get("confirmar", "").strip()

        if senha != confirmar:
            flash("As senhas não coincidem.", "danger")
            return redirect(request.url)

        if len(senha) < 6:
            flash("A senha deve ter pelo menos 6 caracteres.", "warning")
            return redirect(request.url)

        usuario.set_senha(senha)
        db.session.commit()

        flash("Senha redefinida com sucesso.", "success")
        return redirect(url_for("login"))

    return render_template("reset_password.html")




# =========================================================
# ROTAS PRINCIPAIS
# =========================================================
@app.route("/")
@login_required
def index():
    df = listar_empresas_db(current_user.id)

    busca = limpar_texto(request.args.get("busca", ""))
    tipo = limpar_texto(request.args.get("tipo", "")).lower()
    ordem = limpar_texto(request.args.get("ordem", "id_desc"))
    pagina = request.args.get("pagina", 1, type=int)

    df_filtrado = aplicar_filtros(df, busca=busca, tipo=tipo)
    resumo = gerar_resumo(df, df_filtrado)
    df_ordenado = aplicar_ordenacao(df_filtrado, ordem=ordem)
    paginacao = paginar_dataframe(df_ordenado, pagina=pagina, por_pagina=10)

    mensagem_filtro = ""
    categoria_filtro = "primary"

    if tipo == "pessoa":
        mensagem_filtro = f"Filtro aplicado: exibindo somente pessoas ({len(df_filtrado)} resultado(s))."
    elif tipo == "empresa":
        mensagem_filtro = f"Filtro aplicado: exibindo somente empresas ({len(df_filtrado)} resultado(s))."
    elif busca:
        mensagem_filtro = f"Busca aplicada: {len(df_filtrado)} resultado(s) encontrado(s) para '{busca}'."

    return render_template(
        "index.html",
        dados=paginacao["dados"],
        resumo=resumo,
        busca=busca,
        tipo=tipo,
        ordem=ordem,
        pagina=paginacao["pagina"],
        total_paginas=paginacao["total_paginas"],
        total_resultados=paginacao["total_resultados"],
        mensagem_filtro=mensagem_filtro,
        categoria_filtro=categoria_filtro,
    )


@app.route("/add", methods=["POST"])
@login_required
def add():
    novo = montar_registro_form(request.form)

    erros = validar_campos_obrigatorios(novo["Nome"], novo["CPF/CNPJ"])

    if documento_ja_existe_db(novo["CPF/CNPJ"], current_user.id):
        erros.append("Você já possui outro cadastro com este CPF/CNPJ.")

    if erros:
        for erro in erros:
            flash(erro, "danger")
        return redirect(url_for("index"))

    try:
        empresa = Empresa(
            user_id=current_user.id,
            nome=novo["Nome"],
            cpf_cnpj=novo["CPF/CNPJ"],
            celular=novo["Celular"],
            uf=novo["UF"],
            cidade=novo["Cidade"],
            bairro=novo["Bairro"],
            cep=novo["CEP"],
            endereco=novo["Endereço"],
            numero=novo["Número"],
        )

        db.session.add(empresa)
        db.session.commit()

        registrar_auditoria(
            "cadastro_adicionado",
            {
                "id": empresa.id,
                "user_id": current_user.id,
                "nome": empresa.nome,
                "documento": empresa.cpf_cnpj,
            },
        )

        flash("Cadastro salvo com sucesso.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("Erro ao salvar cadastro")
        flash("Erro interno ao salvar cadastro.", "danger")

    return redirect(url_for("index"))


@app.route("/edit/<int:id>")
@login_required
def edit(id):
    empresa = buscar_empresa_db(id, current_user.id)

    if not empresa:
        flash("Cadastro não encontrado.", "danger")
        return redirect(url_for("index"))

    registro = empresa_para_dict(empresa)
    registro["tipo"] = tipo_documento(registro["CPF/CNPJ"])
    registro["documento_numerico"] = apenas_numeros(registro["CPF/CNPJ"])
    registro["celular_numerico"] = apenas_numeros(registro["Celular"])
    registro["cep_numerico"] = apenas_numeros(registro["CEP"])
    registro["completude"] = calcular_completude(registro)

    return render_template("editar.html", dado=registro)


@app.route("/update/<int:id>", methods=["POST"])
@login_required
def update(id):
    empresa = buscar_empresa_db(id, current_user.id)

    if not empresa:
        flash("Cadastro não encontrado.", "danger")
        return redirect(url_for("index"))

    atualizado = montar_registro_form(request.form, id_existente=id)
    erros = validar_campos_obrigatorios(atualizado["Nome"], atualizado["CPF/CNPJ"])

    if documento_ja_existe_db(atualizado["CPF/CNPJ"], current_user.id, ignorar_id=id):
        erros.append("Você já possui outro cadastro com este CPF/CNPJ.")

    if erros:
        for erro in erros:
            flash(erro, "danger")
        return redirect(url_for("edit", id=id))

    antes = empresa_para_dict(empresa)

    try:
        empresa.nome = atualizado["Nome"]
        empresa.cpf_cnpj = atualizado["CPF/CNPJ"]
        empresa.celular = atualizado["Celular"]
        empresa.uf = atualizado["UF"]
        empresa.cidade = atualizado["Cidade"]
        empresa.bairro = atualizado["Bairro"]
        empresa.cep = atualizado["CEP"]
        empresa.endereco = atualizado["Endereço"]
        empresa.numero = atualizado["Número"]

        db.session.commit()

        registrar_auditoria(
            "cadastro_editado",
            {
                "id": str(id),
                "user_id": current_user.id,
                "antes": antes,
                "depois": atualizado,
            },
        )

        flash("Cadastro atualizado com sucesso.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("Erro ao atualizar cadastro")
        flash("Erro interno ao atualizar cadastro.", "danger")

    return redirect(url_for("index"))


@app.route("/delete/<int:id>", methods=["POST"])
@login_required
def delete(id):
    empresa = buscar_empresa_db(id, current_user.id)

    if not empresa:
        flash("Cadastro não encontrado.", "danger")
        return redirect(url_for("index"))

    registro = empresa_para_dict(empresa)

    try:
        db.session.delete(empresa)
        db.session.commit()

        resetar_sequence_empresas_do_usuario_se_vazio(current_user.id)

        registrar_auditoria(
            "cadastro_excluido",
            {
                "id": str(id),
                "user_id": current_user.id,
                "nome": registro["Nome"],
                "documento": registro["CPF/CNPJ"],
            },
        )

        flash("Cadastro excluído com sucesso.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("Erro ao excluir cadastro")
        flash("Erro interno ao excluir cadastro.", "danger")

    return redirect(url_for("index"))


@app.route("/delete_selected", methods=["POST"])
@login_required
def delete_selected():
    ids = request.form.getlist("ids")

    if not ids:
        flash("Nenhum cadastro foi selecionado para exclusão.", "warning")
        return redirect(url_for("index"))

    try:
        ids_int = []
        for item in ids:
            try:
                ids_int.append(int(item))
            except (TypeError, ValueError):
                continue

        if not ids_int:
            flash("Nenhum ID válido foi enviado para exclusão.", "warning")
            return redirect(url_for("index"))

        empresas = Empresa.query.filter(
            Empresa.id.in_(ids_int),
            Empresa.user_id == current_user.id,
        ).all()

        if not empresas:
            flash("Nenhum cadastro encontrado para os itens selecionados.", "warning")
            return redirect(url_for("index"))

        excluidos = []
        for empresa in empresas:
            excluidos.append({
                "id": str(empresa.id),
                "user_id": current_user.id,
                "nome": empresa.nome,
                "documento": empresa.cpf_cnpj,
            })
            db.session.delete(empresa)

        db.session.commit()
        resetar_sequence_empresas_do_usuario_se_vazio(current_user.id)

        registrar_auditoria(
            "cadastros_excluidos_em_massa",
            {
                "quantidade": len(excluidos),
                "user_id": current_user.id,
                "registros": excluidos,
            },
        )

        flash(f"{len(excluidos)} cadastro(s) excluído(s) com sucesso.", "success")

    except Exception:
        db.session.rollback()
        app.logger.exception("Erro ao excluir cadastros selecionados")
        flash("Erro interno ao excluir cadastros selecionados.", "danger")

    return redirect(url_for("index"))


@app.route("/delete_all", methods=["POST"])
@login_required
def delete_all():
    try:
        registros = Empresa.query.filter_by(user_id=current_user.id).all()

        if not registros:
            flash("Você não possui cadastros para excluir.", "warning")
            return redirect(url_for("index"))

        quantidade = len(registros)
        for registro in registros:
            db.session.delete(registro)

        db.session.commit()
        resetar_sequence_empresas_do_usuario_se_vazio(current_user.id)

        registrar_auditoria(
            "base_usuario_resetada",
            {
                "user_id": current_user.id,
                "quantidade": quantidade,
                "acao": "todos os cadastros do usuário foram excluídos",
            },
        )

        flash("Todos os seus cadastros foram excluídos com sucesso.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("Erro ao limpar base do usuário")
        flash("Erro interno ao limpar a base.", "danger")

    return redirect(url_for("index"))


@app.route("/export")
@login_required
def export():
    df = listar_empresas_db(current_user.id)
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Empresas")

    output.seek(0)

    registrar_auditoria(
        "base_exportada",
        {
            "user_id": current_user.id,
            "quantidade_registros": int(len(df)),
        },
    )

    nome_arquivo = f"empresas_usuario_{current_user.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/import", methods=["POST"])
@login_required
def importar():
    arquivos = request.files.getlist("file")

    if not arquivos or all(not arquivo.filename for arquivo in arquivos):
        flash("Selecione ao menos uma planilha .xlsx para importar.", "warning")
        return redirect(url_for("index"))

    df_atual = listar_empresas_db(current_user.id)

    total_importados = 0
    total_ignorados = 0
    total_erros_arquivo = 0
    novos_registros = []

    for arquivo in arquivos:
        if not arquivo or not arquivo.filename:
            continue

        nome = arquivo.filename.lower()
        if not nome.endswith(".xlsx"):
            total_erros_arquivo += 1
            continue

        try:
            df_importado = pd.read_excel(arquivo, dtype=str)
        except Exception:
            total_erros_arquivo += 1
            continue

        base_temp = pd.concat(
            [df_atual, pd.DataFrame(novos_registros)],
            ignore_index=True,
        ) if novos_registros else df_atual

        importados, ignorados = importar_dataframe(df_importado, base_temp)

        novos_registros.extend(importados)
        total_importados += len(importados)
        total_ignorados += ignorados

    if not novos_registros:
        flash(
            f"Nenhum registro novo foi importado. Ignorados: {total_ignorados}. Arquivos com erro: {total_erros_arquivo}.",
            "warning",
        )
        return redirect(url_for("index"))

    try:
        for registro in novos_registros:
            empresa = Empresa(
                user_id=current_user.id,
                nome=registro["Nome"],
                cpf_cnpj=registro["CPF/CNPJ"],
                celular=registro["Celular"],
                uf=registro["UF"],
                cidade=registro["Cidade"],
                bairro=registro["Bairro"],
                cep=registro["CEP"],
                endereco=registro["Endereço"],
                numero=registro["Número"],
            )
            db.session.add(empresa)

        db.session.commit()

        registrar_auditoria(
            "importacao_realizada",
            {
                "user_id": current_user.id,
                "importados": total_importados,
                "ignorados": total_ignorados,
                "erros_arquivo": total_erros_arquivo,
            },
        )

        flash(
            f"Importação concluída: {total_importados} registro(s) importado(s), "
            f"{total_ignorados} ignorado(s) e {total_erros_arquivo} arquivo(s) com erro.",
            "success",
        )
    except Exception:
        db.session.rollback()
        app.logger.exception("Erro ao importar planilha")
        flash("Erro interno ao importar planilha.", "danger")

    return redirect(url_for("index"))


@app.route("/health")
def health():
    return {"status": "ok"}, 200


# =========================================================
# START
# =========================================================
if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        criar_admin_padrao()

    port = int(os.environ.get("PORT", 5000))
    debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug_mode)