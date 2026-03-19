from flask import Flask, render_template, request, redirect, send_file, flash
import pandas as pd
import os
import re

def validar_cpf(cpf):
    cpf = apenas_numeros(cpf)

    if len(cpf) != 11:
        return False

    if cpf == cpf[0] * 11:
        return False

    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    dig1 = (soma * 10) % 11
    dig1 = 0 if dig1 == 10 else dig1

    if dig1 != int(cpf[9]):
        return False

    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    dig2 = (soma * 10) % 11
    dig2 = 0 if dig2 == 10 else dig2

    if dig2 != int(cpf[10]):
        return False

    return True


def validar_cnpj(cnpj):
    cnpj = apenas_numeros(cnpj)

    if len(cnpj) != 14:
        return False

    if cnpj == cnpj[0] * 14:
        return False

    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(int(cnpj[i]) * pesos1[i] for i in range(12))
    resto1 = soma1 % 11
    dig1 = 0 if resto1 < 2 else 11 - resto1

    if dig1 != int(cnpj[12]):
        return False

    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma2 = sum(int(cnpj[i]) * pesos2[i] for i in range(13))
    resto2 = soma2 % 11
    dig2 = 0 if resto2 < 2 else 11 - resto2

    if dig2 != int(cnpj[13]):
        return False

    return True


def validar_documento(valor):
    doc = apenas_numeros(valor)

    if len(doc) == 11:
        return validar_cpf(doc)

    if len(doc) == 14:
        return validar_cnpj(doc)

    return False

def apenas_numeros(valor):
    return re.sub(r"\D", "", str(valor))

def formatar_cpf_cnpj(valor):
    v = apenas_numeros(valor)[:14]

    if len(v) <= 11:
        return f"{v[:3]}.{v[3:6]}.{v[6:9]}-{v[9:11]}"
    else:
        return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:14]}"

def formatar_celular(valor):
    v = apenas_numeros(valor)[:11]
    return f"({v[:2]}){v[2:7]}-{v[7:11]}"

def formatar_cep(valor):
    v = apenas_numeros(valor)[:8]
    return f"{v[:2]}.{v[2:5]}-{v[5:8]}"

app = Flask(__name__)
app.secret_key = "fd1e6978886a5dd23d"

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

    cpf_bruto = request.form["cpf"]
    cpf_limpo = apenas_numeros(cpf_bruto)[:14]

    if not validar_documento(cpf_limpo):
        dados = df.to_dict(orient="records")
        flash("Cadastro realizado com sucesso!", "success")
        return redirect("/")

    cpf_formatado = formatar_cpf_cnpj(cpf_limpo)

    if "CPF/CNPJ" in df.columns:
        docs_existentes = df["CPF/CNPJ"].astype(str).apply(apenas_numeros)
        if cpf_limpo in docs_existentes.values:
            dados = df.to_dict(orient="records")
            return render_template(
                "index.html",
                dados=dados,
                busca="",
                erro="Este CPF/CNPJ já está cadastrado."
            )

    novo = {
        "ID": gerar_id(df),
        "Nome": request.form["nome"],
        "CPF/CNPJ": cpf_formatado,
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

    flash('Cadastro realizado com sucesso!','Sucess')
    return redirect("/")

@app.route("/delete/<int:id>")
def delete(id):
    df = carregar()
    df = df[df["ID"] != id]
    salvar(df)

    flash('Cadastro excluído com sucesso!','Sucess')
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

    cpf_bruto = request.form["cpf"]
    cpf_limpo = apenas_numeros(cpf_bruto)[:14]

    if not validar_documento(cpf_limpo):
        empresa_df = df[df["ID"] == id]

        if empresa_df.empty:
            return "Empresa não encontrada"

        empresa = empresa_df.iloc[0].to_dict()
        empresa["Nome"] = request.form["nome"]
        empresa["CPF/CNPJ"] = request.form["cpf"]
        empresa["Celular"] = request.form["celular"]
        empresa["UF"] = request.form["uf"]
        empresa["Cidade"] = request.form["cidade"]
        empresa["Bairro"] = request.form["bairro"]
        empresa["CEP"] = request.form["cep"]
        empresa["Endereço"] = request.form["endereco"]
        empresa["Número"] = request.form["numero"]

        return render_template("editar.html", e=empresa, erro="CPF/CNPJ inválido.")

    docs_existentes = df[df["ID"] != id]["CPF/CNPJ"].astype(str).apply(apenas_numeros)
    if cpf_limpo in docs_existentes.values:
        empresa_df = df[df["ID"] == id]

        if empresa_df.empty:
            return "Empresa não encontrada"

        empresa = empresa_df.iloc[0].to_dict()
        empresa["Nome"] = request.form["nome"]
        empresa["CPF/CNPJ"] = request.form["cpf"]
        empresa["Celular"] = request.form["celular"]
        empresa["UF"] = request.form["uf"]
        empresa["Cidade"] = request.form["cidade"]
        empresa["Bairro"] = request.form["bairro"]
        empresa["CEP"] = request.form["cep"]
        empresa["Endereço"] = request.form["endereco"]
        empresa["Número"] = request.form["numero"]

        return render_template("editar.html", e=empresa, erro="Este CPF/CNPJ já está cadastrado.")

    for i, row in df.iterrows():
        if row["ID"] == id:
            df.at[i, "Nome"] = request.form["nome"]
            df.at[i, "CPF/CNPJ"] = formatar_cpf_cnpj(cpf_limpo)
            df.at[i, "Celular"] = formatar_celular(request.form["celular"])
            df.at[i, "Cidade"] = request.form["cidade"]
            df.at[i, "UF"] = request.form["uf"]
            df.at[i, "Bairro"] = request.form["bairro"]
            df.at[i, "CEP"] = formatar_cep(request.form["cep"])
            df.at[i, "Endereço"] = request.form["endereco"]
            df.at[i, "Número"] = request.form["numero"]
            break

    salvar(df)

    flash('Dados Atualizados com sucesso!','Sucess')
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
