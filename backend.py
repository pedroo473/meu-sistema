from flask import Flask, render_template, request, redirect, send_file
import pandas as pd
import os
import re

def apenas_numeros(valor):
    return re.sub(r"\D", "", str(valor))

def formatar_cpf_cnpj(valor):
    v = apenas_numeros(valor)

    if len(v) <= 11:
        # CPF
        return f"{v[:3]}.{v[3:6]}.{v[6:9]}-{v[9:11]}"
    else:
        # CNPJ
        return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:14]}"

def formatar_celular(valor):
    v = apenas_numeros(valor)[:11]
    return f"({v[:2]}){v[2:7]}-{v[7:11]}"

def formatar_cep(valor):
    v = apenas_numeros(valor)[:8]
    return f"{v[:2]}.{v[2:5]}-{v[5:8]}"

app = Flask(__name__)

ARQUIVO = "empresas.xlsx"

COLUNAS = [
    "ID", "Nome", "CPF/CNPJ", "Celular", "UF",
    "Cidade", "Bairro", "CEP", "Endereço", "Número"
]

def normalizar_colunas(df):
    # tira espaços extras
    df.columns = df.columns.str.strip()

    # corrige nomes errados
    df = df.rename(columns={
        "CPF / CNPJ": "CPF/CNPJ",
        "CPF CNPJ": "CPF/CNPJ",
        "cpf/cnpj": "CPF/CNPJ",
        "cpf / cnpj": "CPF/CNPJ",
        "CPF/CNPJ ": "CPF/CNPJ",
        " Endereço": "Endereço",
        "Endereco": "Endereço",
        "Enderço": "Endereço",
        "endereco": "Endereço",
        "Endereço ": "Endereço",
        "numero": "Número",
        "Numero": "Número",
        " nome": "Nome",
        "nome": "Nome",
        "celular ": "Celular",
        "uf": "UF",
        "cidade": "Cidade",
        "bairro": "Bairro",
        "cep": "CEP",
        "id": "ID"
    })

    # remove colunas duplicadas pelo nome
    df = df.loc[:, ~df.columns.duplicated()]

    # cria colunas faltantes
    for coluna in COLUNAS:
        if coluna not in df.columns:
            df[coluna] = ""

    # mantém somente as colunas corretas e na ordem certa
    df = df[COLUNAS]

    return df

def salvar(df):
    df = normalizar_colunas(df)
    df.to_excel(ARQUIVO, index=False)

def gerar_id(df):
    if df.empty:
        return 1

    ids_validos = pd.to_numeric(df["ID"], errors="coerce").dropna()

    if ids_validos.empty:
        return 1

    return int(ids_validos.max()) + 1

def carregar():
    if os.path.exists(ARQUIVO):
        df = pd.read_excel(ARQUIVO, engine="openpyxl")
        df = normalizar_colunas(df)

        if df["ID"].isnull().all() or pd.to_numeric(df["ID"], errors="coerce").isnull().all():
            df["ID"] = range(1, len(df) + 1)
            salvar(df)

        return df

    return pd.DataFrame(columns=COLUNAS)

@app.route("/")
def index():
    busca = request.args.get("busca", "").lower()
    df = carregar()

    if busca:
        df = df[df.apply(lambda x: busca in str(x).lower(), axis=1)]

    dados = df.to_dict(orient="records")
    return render_template("index.html", dados=dados, busca=busca)

@app.route("/add", methods=["POST"])
def add():
    df = carregar()

    novo = {
        "ID": gerar_id(df),
        "Nome": request.form["nome"],
        "CPF/CNPJ": formatar_cpf_cnpj(request.form["cpf"]),
        "Celular": formatar_celular(request.form["celular"]),
        "UF": request.form["uf"],
        "Cidade": request.form["cidade"],
        "Bairro": request.form["bairro"],
        "CEP": formatar_cep(request.form["cep"]),
        "Endereço": request.form["endereco"],
        "Número": request.form["numero"]
    }

    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
    salvar(df)
    return redirect("/")

@app.route("/delete/<int:id>")
def delete(id):
    df = carregar()
    df = df[df["ID"] != id]
    salvar(df)
    return redirect("/")

@app.route("/edit/<int:id>")
def edit(id):
    df = carregar()
    empresa_df = df[df["ID"] == id]

    if empresa_df.empty:
        return "Empresa não encontrada"

    empresa = empresa_df.iloc[0].to_dict()
    return render_template("editar.html", e=empresa)

@app.route("/update/<int:id>", methods=["POST"])
def update(id):
    df = carregar()

    for i, row in df.iterrows():
        if row["ID"] == id:
            df.at[i, "Nome"] = request.form["nome"]
            df.at[i, "CPF/CNPJ"] = formatar_cpf_cnpj(request.form["cpf"])
            df.at[i, "Celular"] = formatar_celular(request.form["celular"])
            df.at[i, "Cidade"] = request.form["cidade"]
            df.at[i, "UF"] = request.form["uf"]
            df.at[i, "Bairro"] = request.form["bairro"]
            df.at[i, "CEP"] = formatar_cep(request.form["cep"])
            df.at[i, "Endereço"] = request.form["endereco"]
            df.at[i, "Número"] = request.form["numero"]
            break

    salvar(df)
    return redirect("/")

@app.route("/export")
def export():
    df = carregar()
    arquivo_exportado = "empresas_exportadas.xlsx"
    df.to_excel(arquivo_exportado, index=False)
    return send_file(arquivo_exportado, as_attachment=True)

@app.route("/import", methods=["POST"])
def importar():
    if "file" not in request.files:
        return "Nenhum arquivo foi enviado."

    file = request.files["file"]

    if file.filename == "":
        return "Nenhum arquivo foi selecionado."

    if not file.filename.lower().endswith(".xlsx"):
        return "Envie apenas arquivos .xlsx"

    try:
        df = pd.read_excel(file, engine="openpyxl")
        df = normalizar_colunas(df)

        if "ID" not in df.columns or df["ID"].isnull().all():
            df["ID"] = range(1, len(df) + 1)

        salvar(df)
        return redirect("/")

    except Exception as e:
        return f"Erro ao importar arquivo: {str(e)}"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
