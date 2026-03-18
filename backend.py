from flask import Flask, render_template, request, redirect, send_file
import pandas as pd
import os

app = Flask(__name__)

ARQUIVO = "empresas.xlsx"

COLUNAS = [
    "ID", "Nome", "CPF/CNPJ", "Celular", "UF",
    "Cidade", "Bairro", "CEP", "Endereço", "Número"
]

# 🔧 FUNÇÕES FORA (CORRETO)
def salvar(df):
    df.to_excel(ARQUIVO, index=False)

def gerar_id(df):
    if df.empty:
        return 1
    return int(df["ID"].max()) + 1

def carregar():
    if os.path.exists(ARQUIVO):
        df = pd.read_excel(ARQUIVO)

        df.columns = df.columns.str.strip()

        if "ID" not in df.columns:
            df["ID"] = range(1, len(df) + 1)
            salvar(df)

        return df

    return pd.DataFrame(columns=COLUNAS)

# 🔎 LISTAR + BUSCAR
@app.route("/")
def index():
    busca = request.args.get("busca", "").lower()
    df = carregar()

    if busca:
        df = df[df.apply(lambda x: busca in str(x).lower(), axis=1)]

    dados = df.to_dict(orient="records")
    return render_template("index.html", dados=dados, busca=busca)

# ➕ ADICIONAR
@app.route("/add", methods=["POST"])
def add():
    df = carregar()

    novo = {
        "ID": gerar_id(df),
        "Nome": request.form["nome"],
        "CPF/CNPJ": request.form["cpf"],
        "Celular": request.form["celular"],
        "UF": request.form["uf"],
        "Cidade": request.form["cidade"],
        "Bairro": request.form["bairro"],
        "CEP": request.form["cep"],
        "Endereço": request.form["endereco"],
        "Número": request.form["numero"]
    }

    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
    salvar(df)
    return redirect("/")

# ❌ EXCLUIR
@app.route("/delete/<int:id>")
def delete(id):
    df = carregar()
    df = df[df["ID"] != id]
    salvar(df)
    return redirect("/")

# ✏️ EDITAR
@app.route("/edit/<int:id>")
def edit(id):
    df = carregar()
    empresa_df = df[df["ID"] == id]

    if empresa_df.empty:
        return "Empresa não encontrada"

    empresa = empresa_df.iloc[0].to_dict()
    return render_template("editar.html", e=empresa)

# 💾 UPDATE
@app.route("/update/<int:id>", methods=["POST"])
def update(id):
    df = carregar()

    for i, row in df.iterrows():
        if row["ID"] == id:
            df.at[i, "Nome"] = request.form["nome"]
            df.at[i, "CPF/CNPJ"] = request.form["cpf"]
            df.at[i, "Celular"] = request.form["celular"]
            df.at[i, "Cidade"] = request.form["cidade"]
            df.at[i, "UF"] = request.form["uf"]
            df.at[i, "Bairro"] = request.form["bairro"]
            df.at[i, "CEP"] = request.form["cep"]
            df.at[i, "Endereço"] = request.form["endereco"]
            df.at[i, "Número"] = request.form["numero"]

    salvar(df)
    return redirect("/")

# 📥 EXPORTAR
@app.route("/export")
def export():
    return send_file(ARQUIVO, as_attachment=True)

# 📤 IMPORTAR
@app.route("/import", methods=["POST"])
def importar():
    file = request.files["file"]
    df = pd.read_excel(file)

    df.columns = df.columns.str.strip()

    if "ID" not in df.columns:
        df["ID"] = range(1, len(df) + 1)

    salvar(df)
    return redirect("/")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)