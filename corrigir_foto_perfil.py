import sqlite3

conn = sqlite3.connect("local.db")
cursor = conn.cursor()

try:
    cursor.execute("ALTER TABLE usuarios ADD COLUMN foto_perfil VARCHAR(255) DEFAULT 'default.png'")
    conn.commit()
    print("Coluna foto_perfil adicionada com sucesso.")
except Exception as e:
    print("Erro:", e)

conn.close()