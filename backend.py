from flask import Flask, render_template, request, redirect, send_file, flash, url_for
import pandas as pd
import os
import re
import json
import shutil
from datetime import datetime
from io import BytesIO

app = Flask(__name__)
app.secret_key = "fd1e6978886a5dd23d"

ARQUIVO = "empresas.xlsx"
PASTA_BACKUP = "backups"
ARQUIVO_AUDITORIA = "auditoria.jsonl"

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


def garantir_pastas():
    os.makedirs(PASTA_BACKUP, exist_ok=True)


def garantir_arquivo_base():
    garantir_pastas()
    if not os.path.exists(ARQUIVO):
        df = pd.DataFrame(columns=COLUNAS)
        df.to_excel(ARQUIVO, index=False)


def registrar_auditoria(acao, detalhes=None):
    garantir_pastas()
    payload = {
        "data_hora": timestamp_agora(),
        "acao": acao,
        "detalhes": detalhes or {},
    }
    try:
        with open(ARQUIVO_AUDITORIA, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


def criar_backup(motivo="alteracao"):
    garantir_arquivo_base()
    garantir_pastas()

    if not os.path.exists(ARQUIVO):
        return None

    nome_backup = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{motivo}.xlsx"
    destino = os.path.join(PASTA_BACKUP, nome_backup)

    try:
        shutil.copy2(ARQUIVO, destino)
        return destino
    except Exception:
        return None


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
# LEITURA / ESCRITA DA BASE
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


def ler_dados():
    garantir_arquivo_base()

    try:
        df = pd.read_excel(ARQUIVO, dtype=str)
    except Exception:
        df = pd.DataFrame(columns=COLUNAS)

    df = normalizar_dataframe(df)

    ids_validos = []
    proximo = 1

    for valor in df["ID"]:
        numeros = apenas_numeros(valor)
        if numeros:
            ids_validos.append(str(int(numeros)))
        else:
            ids_validos.append(str(proximo))
            proximo += 1

    df["ID"] = ids_validos
    return df


def salvar_dados(df, motivo_backup="alteracao"):
    df = normalizar_dataframe(df)

    backup_criado = criar_backup(motivo_backup)
    if backup_criado:
        registrar_auditoria("backup_criado", {"arquivo": backup_criado})

    df.to_excel(ARQUIVO, index=False)


def proximo_id(df):
    if df.empty:
        return 1

    ids = pd.to_numeric(df["ID"], errors="coerce").dropna()
    if ids.empty:
        return 1

    return int(ids.max()) + 1


def buscar_por_id(df, id_registro):
    filtro = df["ID"].astype(str) == str(id_registro)
    resultado = df[filtro]
    if resultado.empty:
        return None, None
    idx = resultado.index[0]
    return idx, resultado.iloc[0].to_dict()


def documento_ja_existe(df, documento, ignorar_id=None):
    doc_limpo = apenas_numeros(documento)

    for _, linha in df.iterrows():
        id_linha = str(linha["ID"])
        doc_linha = apenas_numeros(linha["CPF/CNPJ"])
        if ignorar_id is not None and id_linha == str(ignorar_id):
            continue
        if doc_linha and doc_linha == doc_limpo:
            return True
    return False


# =========================================================
# RESUMO / FILTRO / ORDENAÇÃO / PAGINAÇÃO
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
        df_ordenado = df_ordenado.drop(columns=["_id_num"])
        return df_ordenado

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
    df_ordenado = df_ordenado.drop(columns=["_id_num"])
    return df_ordenado


def gerar_resumo(df_total, df_filtrado):
    documentos = df_total["CPF/CNPJ"].astype(str).apply(apenas_numeros)

    total_pessoas = int((documentos.str.len() == 11).sum())
    total_empresas = int((documentos.str.len() == 14).sum())

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
    registro = {
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
    return registro


def lista_erros_registro(df, registro, ignorar_id=None):
    erros = validar_campos_obrigatorios(registro["Nome"], registro["CPF/CNPJ"])

    if documento_ja_existe(df, registro["CPF/CNPJ"], ignorar_id=ignorar_id):
        erros.append("Já existe outro cadastro com este CPF/CNPJ.")

    return erros


# =========================================================
# IMPORTAÇÃO
# =========================================================

def detectar_coluna(df, *nomes):
    mapa = {normalizar_chave_coluna(col): col for col in df.columns}
    for nome in nomes:
        chave = normalizar_chave_coluna(nome)
        if chave in mapa:
            return mapa[chave]
    return None


def valor_coluna(linha, coluna):
    if coluna is None:
        return ""
    return linha.get(coluna, "")


def importar_dataframe(df_origem, df_atual, proximo_codigo):
    df_origem = normalizar_dataframe(df_origem)

    importados = []
    ignorados = 0

    docs_existentes = set(df_atual["CPF/CNPJ"].astype(str).apply(apenas_numeros).tolist())

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
            "ID": str(proximo_codigo),
            "Nome": nome,
            "CPF/CNPJ": documento,
            "Celular": celular,
            "UF": uf,
            "Cidade": cidade,
            "Bairro": bairro,
            "CEP": cep,
            "Endereço": endereco,
            "Número": numero,
        }

        importados.append(registro)
        docs_existentes.add(doc_limpo)
        proximo_codigo += 1

    return importados, ignorados, proximo_codigo


# =========================================================
# ROTAS
# =========================================================

@app.route("/")
def index():
    df = ler_dados()

    busca = limpar_texto(request.args.get("busca", ""))
    tipo = limpar_texto(request.args.get("tipo", "")).lower()
    ordem = limpar_texto(request.args.get("ordem", "id_desc"))
    pagina = request.args.get("pagina", 1, type=int)

    df_filtrado = aplicar_filtros(df, busca=busca, tipo=tipo)
    resumo = gerar_resumo(df, df_filtrado)
    df_ordenado = aplicar_ordenacao(df_filtrado, ordem=ordem)
    paginacao = paginar_dataframe(df_ordenado, pagina=pagina, por_pagina=10)

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
    )


@app.route("/add", methods=["POST"])
def add():
    df = ler_dados()
    novo = montar_registro_form(request.form)
    novo["ID"] = str(proximo_id(df))

    erros = lista_erros_registro(df, novo)

    if erros:
        for erro in erros:
            flash(erro, "danger")
        return redirect(url_for("index"))

    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
    salvar_dados(df, motivo_backup="add")

    registrar_auditoria(
        "cadastro_adicionado",
        {
            "id": novo["ID"],
            "nome": novo["Nome"],
            "documento": novo["CPF/CNPJ"],
        },
    )

    flash("Cadastro salvo com sucesso.", "success")
    return redirect(url_for("index"))


@app.route("/edit/<int:id>")
def edit(id):
    df = ler_dados()
    idx, registro = buscar_por_id(df, id)

    if registro is None:
        flash("Cadastro não encontrado.", "danger")
        return redirect(url_for("index"))

    registro["tipo"] = tipo_documento(registro["CPF/CNPJ"])
    registro["documento_numerico"] = apenas_numeros(registro["CPF/CNPJ"])
    registro["celular_numerico"] = apenas_numeros(registro["Celular"])
    registro["cep_numerico"] = apenas_numeros(registro["CEP"])
    registro["completude"] = calcular_completude(registro)

    return render_template("editar.html", dado=registro)


@app.route("/update/<int:id>", methods=["POST"])
def update(id):
    df = ler_dados()
    idx, atual = buscar_por_id(df, id)

    if atual is None:
        flash("Cadastro não encontrado.", "danger")
        return redirect(url_for("index"))

    atualizado = montar_registro_form(request.form, id_existente=id)
    erros = lista_erros_registro(df, atualizado, ignorar_id=id)

    if erros:
        for erro in erros:
            flash(erro, "danger")
        return redirect(url_for("edit", id=id))

    for coluna in COLUNAS:
        df.at[idx, coluna] = atualizado[coluna]

    salvar_dados(df, motivo_backup="update")

    registrar_auditoria(
        "cadastro_editado",
        {
            "id": str(id),
            "antes": atual,
            "depois": atualizado,
        },
    )

    flash("Cadastro atualizado com sucesso.", "success")
    return redirect(url_for("index"))


@app.route("/delete/<int:id>")
def delete(id):
    df = ler_dados()
    idx, registro = buscar_por_id(df, id)

    if registro is None:
        flash("Cadastro não encontrado.", "danger")
        return redirect(url_for("index"))

    df = df[df["ID"].astype(str) != str(id)]
    salvar_dados(df, motivo_backup="delete")

    registrar_auditoria(
        "cadastro_excluido",
        {
            "id": str(id),
            "nome": registro["Nome"],
            "documento": registro["CPF/CNPJ"],
        },
    )

    flash("Cadastro excluído com sucesso.", "success")
    return redirect(url_for("index"))


@app.route("/delete_selected", methods=["POST"])
def delete_selected():
    ids = request.form.getlist("ids")

    if not ids:
        flash("Selecione ao menos um cadastro para excluir.", "warning")
        return redirect(url_for("index"))

    df = ler_dados()
    ids_set = {str(i) for i in ids}

    registros_excluidos = df[df["ID"].astype(str).isin(ids_set)].to_dict(orient="records")
    qtd_excluir = len(registros_excluidos)

    if qtd_excluir == 0:
        flash("Nenhum cadastro válido foi encontrado para exclusão.", "warning")
        return redirect(url_for("index"))

    df = df[~df["ID"].astype(str).isin(ids_set)]
    salvar_dados(df, motivo_backup="delete_selected")

    registrar_auditoria(
        "cadastros_excluidos_em_lote",
        {
            "quantidade": qtd_excluir,
            "ids": list(ids_set),
            "registros": registros_excluidos,
        },
    )

    flash(f"{qtd_excluir} cadastro(s) excluído(s) com sucesso.", "success")
    return redirect(url_for("index"))


@app.route("/export")
def export():
    df = ler_dados()
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Empresas")

    output.seek(0)

    registrar_auditoria(
        "base_exportada",
        {
            "quantidade_registros": int(len(df)),
        },
    )

    nome_arquivo = f"empresas_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/import", methods=["POST"])
def importar():
    arquivos = request.files.getlist("file")

    if not arquivos or all(not arquivo.filename for arquivo in arquivos):
        flash("Selecione ao menos uma planilha .xlsx para importar.", "warning")
        return redirect(url_for("index"))

    df_atual = ler_dados()
    proximo_codigo = proximo_id(df_atual)

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

        importados, ignorados, proximo_codigo = importar_dataframe(
            df_importado,
            pd.concat([df_atual, pd.DataFrame(novos_registros)], ignore_index=True)
            if novos_registros else df_atual,
            proximo_codigo
        )

        novos_registros.extend(importados)
        total_importados += len(importados)
        total_ignorados += ignorados

    if novos_registros:
        df_final = pd.concat([df_atual, pd.DataFrame(novos_registros)], ignore_index=True)
        salvar_dados(df_final, motivo_backup="import")

        registrar_auditoria(
            "importacao_realizada",
            {
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
    else:
        flash(
            f"Nenhum registro novo foi importado. "
            f"Ignorados: {total_ignorados}. Arquivos com erro: {total_erros_arquivo}.",
            "warning",
        )

    return redirect(url_for("index"))


# =========================================================
# FUNÇÕES EXTRAS DA TELA DE EDIÇÃO
# =========================================================

def calcular_completude(registro):
    campos = [
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

    preenchidos = 0
    for campo in campos:
        if limpar_texto(registro.get(campo)):
            preenchidos += 1

    percentual = int(round((preenchidos / len(campos)) * 100))
    return percentual


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
