from flask import Flask, render_template, request, redirect, send_file, flash, url_for
import pandas as pd
import os
import re

app = Flask(__name__)
app.secret_key = "fd1e6978886a5dd23d"

ARQUIVO = "empresas.xlsx"

COLUNAS = [
    "ID", "Nome", "CPF/CNPJ", "Celular", "UF",
    "Cidade", "Bairro", "CEP", "Endereço", "Número"
]


def apenas_numeros(valor):
    return re.sub(r"\D", "", str(valor))


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


def formatar_cpf_cnpj(valor):
    v = apenas_numeros(valor)[:14]

    if len(v) <= 11:
        if len(v) == 11:
            return f"{v[:3]}.{v[3:6]}.{v[6:9]}-{v[9:11]}"
        return v
    else:
        if len(v) == 14:
            return f"{v[:2]}.{v[2:5]}.{v[5:8]}/{v[8:12]}-{v[12:14]}"
        return v


def formatar_celular(valor):
    v = apenas_numeros(valor)[:11]

    if not v:
        return ""

    if len(v) == 11:
        return f"({v[:2]}){v[2:7]}-{v[7:11]}"

    if len(v) == 10:
        return f"({v[:2]}){v[2:6]}-{v[6:10]}"

    return v


def formatar_cep(valor):
    v = apenas_numeros(valor)[:8]

    if not v:
        return ""

    if len(v) == 8:
        return f"{v[:2]}.{v[2:5]}-{v[5:8]}"

    return v


def normalizar_colunas(df):
    df.columns = df.columns.str.strip()

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

    df = df.loc[:, ~df.columns.duplicated()]

    for coluna in COLUNAS:
        if coluna not in df.columns:
            df[coluna] = ""

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
    busca = request.args.get("busca", "").strip().lower()
    filtro_tipo = request.args.get("tipo", "").strip().lower()

    df = carregar()

    if busca:
        df = df[df.apply(lambda x: busca in " ".join(x.astype(str)).lower(), axis=1)]

    if filtro_tipo == "pessoa":
        df = df[df["CPF/CNPJ"].astype(str).apply(lambda x: len(apenas_numeros(x)) == 11)]

    elif filtro_tipo == "empresa":
        df = df[df["CPF/CNPJ"].astype(str).apply(lambda x: len(apenas_numeros(x)) == 14)]

    dados = df.to_dict(orient="records")
    return render_template(
        "index.html",
        dados=dados,
        busca=request.args.get("busca", ""),
        tipo=filtro_tipo
    )


@app.route("/add", methods=["POST"])
def add():
    try:
        df = carregar()

        nome = request.form.get("nome", "").strip()
        cpf_bruto = request.form.get("cpf_cnpj", "").strip()
        celular = request.form.get("celular", "").strip()
        uf = request.form.get("uf", "").strip()
        cidade = request.form.get("cidade", "").strip()
        bairro = request.form.get("bairro", "").strip()
        cep = request.form.get("cep", "").strip()
        endereco = request.form.get("endereco", "").strip()
        numero = request.form.get("numero", "").strip()

        cpf_limpo = apenas_numeros(cpf_bruto)[:14]

        if not nome:
            flash("O campo Nome é obrigatório.", "danger")
            return redirect(url_for("index"))

        if not validar_documento(cpf_limpo):
            flash("CPF/CNPJ inválido.", "danger")
            return redirect(url_for("index"))

        if "CPF/CNPJ" in df.columns:
            docs_existentes = df["CPF/CNPJ"].astype(str).apply(apenas_numeros)
            if cpf_limpo in docs_existentes.values:
                flash("Este CPF/CNPJ já está cadastrado.", "warning")
                return redirect(url_for("index"))

        novo = {
            "ID": gerar_id(df),
            "Nome": nome,
            "CPF/CNPJ": formatar_cpf_cnpj(cpf_limpo),
            "Celular": formatar_celular(celular),
            "UF": uf,
            "Cidade": cidade,
            "Bairro": bairro,
            "CEP": formatar_cep(cep),
            "Endereço": endereco,
            "Número": numero
        }

        df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
        salvar(df)

        flash("Cadastro realizado com sucesso!", "success")
        return redirect(url_for("index"))

    except Exception as e:
        flash(f"Erro ao cadastrar: {str(e)}", "danger")
        return redirect(url_for("index"))


@app.route("/delete/<int:id>")
def delete(id):
    try:
        df = carregar()
        qtd_antes = len(df)

        df = df[df["ID"] != id]

        if len(df) == qtd_antes:
            flash("Cadastro não encontrado.", "warning")
            return redirect(url_for("index"))

        salvar(df)
        flash("Cadastro excluído com sucesso!", "danger")
        return redirect(url_for("index"))

    except Exception as e:
        flash(f"Erro ao excluir: {str(e)}", "danger")
        return redirect(url_for("index"))


@app.route("/delete_selected", methods=["POST"])
def delete_selected():
    try:
        ids_selecionados = request.form.getlist("ids")

        if not ids_selecionados:
            flash("Nenhum cadastro foi selecionado.", "warning")
            return redirect(url_for("index"))

        ids_selecionados = [int(i) for i in ids_selecionados]

        df = carregar()
        qtd_antes = len(df)

        df = df[~df["ID"].isin(ids_selecionados)]
        qtd_excluidos = qtd_antes - len(df)

        if qtd_excluidos == 0:
            flash("Nenhum cadastro selecionado foi encontrado.", "warning")
            return redirect(url_for("index"))

        salvar(df)
        flash(f"{qtd_excluidos} cadastro(s) excluído(s) com sucesso!", "danger")
        return redirect(url_for("index"))

    except Exception as e:
        flash(f"Erro ao excluir selecionados: {str(e)}", "danger")
        return redirect(url_for("index"))


@app.route("/edit/<int:id>")
def edit(id):
    df = carregar()
    empresa_df = df[df["ID"] == id]

    if empresa_df.empty:
        flash("Empresa não encontrada.", "danger")
        return redirect(url_for("index"))

    empresa = empresa_df.iloc[0].to_dict()
    return render_template("editar.html", e=empresa)


@app.route("/update/<int:id>", methods=["POST"])
def update(id):
    try:
        df = carregar()
        empresa_df = df[df["ID"] == id]

        if empresa_df.empty:
            flash("Empresa não encontrada.", "danger")
            return redirect(url_for("index"))

        nome = request.form.get("nome", "").strip()
        cpf_bruto = request.form.get("cpf_cnpj", "").strip()
        celular = request.form.get("celular", "").strip()
        uf = request.form.get("uf", "").strip()
        cidade = request.form.get("cidade", "").strip()
        bairro = request.form.get("bairro", "").strip()
        cep = request.form.get("cep", "").strip()
        endereco = request.form.get("endereco", "").strip()
        numero = request.form.get("numero", "").strip()

        cpf_limpo = apenas_numeros(cpf_bruto)[:14]

        if not nome:
            empresa = empresa_df.iloc[0].to_dict()
            empresa["Nome"] = nome
            empresa["CPF/CNPJ"] = cpf_bruto
            empresa["Celular"] = celular
            empresa["UF"] = uf
            empresa["Cidade"] = cidade
            empresa["Bairro"] = bairro
            empresa["CEP"] = cep
            empresa["Endereço"] = endereco
            empresa["Número"] = numero
            return render_template("editar.html", e=empresa, erro="O campo Nome é obrigatório.")

        if not validar_documento(cpf_limpo):
            empresa = empresa_df.iloc[0].to_dict()
            empresa["Nome"] = nome
            empresa["CPF/CNPJ"] = cpf_bruto
            empresa["Celular"] = celular
            empresa["UF"] = uf
            empresa["Cidade"] = cidade
            empresa["Bairro"] = bairro
            empresa["CEP"] = cep
            empresa["Endereço"] = endereco
            empresa["Número"] = numero
            return render_template("editar.html", e=empresa, erro="CPF/CNPJ inválido.")

        docs_existentes = df[df["ID"] != id]["CPF/CNPJ"].astype(str).apply(apenas_numeros)
        if cpf_limpo in docs_existentes.values:
            empresa = empresa_df.iloc[0].to_dict()
            empresa["Nome"] = nome
            empresa["CPF/CNPJ"] = cpf_bruto
            empresa["Celular"] = celular
            empresa["UF"] = uf
            empresa["Cidade"] = cidade
            empresa["Bairro"] = bairro
            empresa["CEP"] = cep
            empresa["Endereço"] = endereco
            empresa["Número"] = numero
            return render_template("editar.html", e=empresa, erro="Este CPF/CNPJ já está cadastrado.")

        for i, row in df.iterrows():
            if row["ID"] == id:
                df.at[i, "Nome"] = nome
                df.at[i, "CPF/CNPJ"] = formatar_cpf_cnpj(cpf_limpo)
                df.at[i, "Celular"] = formatar_celular(celular)
                df.at[i, "UF"] = uf
                df.at[i, "Cidade"] = cidade
                df.at[i, "Bairro"] = bairro
                df.at[i, "CEP"] = formatar_cep(cep)
                df.at[i, "Endereço"] = endereco
                df.at[i, "Número"] = numero
                break

        salvar(df)
        flash("Dados atualizados com sucesso!", "primary")
        return redirect(url_for("index"))

    except Exception as e:
        flash(f"Erro ao atualizar: {str(e)}", "danger")
        return redirect(url_for("index"))


@app.route("/export")
def export():
    try:
        df = carregar()
        arquivo_exportado = "empresas_exportadas.xlsx"
        df.to_excel(arquivo_exportado, index=False)
        return send_file(arquivo_exportado, as_attachment=True)

    except Exception as e:
        flash(f"Erro ao exportar: {str(e)}", "danger")
        return redirect(url_for("index"))


@app.route("/import", methods=["POST"])
def importar():
    if "file" not in request.files:
        flash("Nenhum arquivo foi enviado.", "danger")
        return redirect(url_for("index"))

    file = request.files["file"]

    if file.filename == "":
        flash("Nenhum arquivo foi selecionado.", "danger")
        return redirect(url_for("index"))

    if not file.filename.lower().endswith(".xlsx"):
        flash("Envie apenas arquivos .xlsx", "warning")
        return redirect(url_for("index"))

    try:
        df_importado = pd.read_excel(file, engine="openpyxl")
        df_importado = normalizar_colunas(df_importado)

        if "ID" not in df_importado.columns or df_importado["ID"].isnull().all():
            df_importado["ID"] = range(1, len(df_importado) + 1)

        docs_importados = df_importado["CPF/CNPJ"].astype(str).apply(apenas_numeros)
        docs_importados_validos = docs_importados[docs_importados != ""]

        if docs_importados_validos.duplicated().any():
            flash("A planilha importada contém CPF/CNPJ duplicado.", "warning")
            return redirect(url_for("index"))

        df_atual = carregar()
        docs_atuais = df_atual["CPF/CNPJ"].astype(str).apply(apenas_numeros)

        repetidos = set(docs_importados_validos).intersection(set(docs_atuais))
        if repetidos:
            flash("A planilha possui CPF/CNPJ que já existem no sistema.", "warning")
            return redirect(url_for("index"))

        salvar(df_importado)
        flash("Arquivo importado com sucesso!", "success")
        return redirect(url_for("index"))

    except Exception as e:
        flash(f"Erro ao importar arquivo: {str(e)}", "danger")
        return redirect(url_for("index"))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)