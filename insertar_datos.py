import psycopg2
import pandas as pd

# Conexión a la base de datos PostgreSQL
conn = psycopg2.connect(
    dbname="railway",
    user="postgres",
    password="EXLqFmddMzSSPLELXtdrHzzomFAOTirS",
    host="autorack.proxy.rlwy.net",
    port="54979"
)
cur = conn.cursor()

# Cargar el archivo CSV
file_path = 'datos.csv'
data = pd.read_csv(file_path)

# Función para convertir fechas de formato dd/mm/yyyy a yyyy-mm-dd
def convertir_fecha(fecha):
    if pd.isna(fecha):
        return None
    try:
        fecha_convertida = pd.to_datetime(fecha, format='%d/%m/%Y', errors='coerce')
        if pd.isna(fecha_convertida):
            return None
        return fecha_convertida.date()
    except ValueError:
        return None

# Preparar y ejecutar las consultas de inserción
for index, row in data.iterrows():
    cur.execute("""
        INSERT INTO pqrsf (
            numpro, fecha_creacion, categoria, localidad, numrad, fecrad,
            vencimiento_dias, tipo_tercero, numter, nomter, numero_sdqs,
            tipo_documento, tema, tipo_afectacion, medio_envio, fecha_vencimiento,
            respuesta_parcial, fecha_resp_parcial, comunicacion_aviso_parcial,
            fecha_publicacion_aviso_parcial, fecha_desfijacion_parcial,
            respuesta_final, fecha_respuesta, comunicado_aviso_final,
            fecha_publicacion_aviso_final, fecha_desfijacion_aviso_final,
            fecha_finalizacion_proceso, respuesta_oportuna, comentario,
            dependencia_responsable, estado, vencido
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['NUMPRO'], convertir_fecha(row['FECHA_CREACION']), row['CATEGORIA'], row['LOCALIDAD'],
        row['NUMRAD'], convertir_fecha(row['FECRAD']), row['VENCIMIENTO_DIAS'], row['TIPO_TERCERO'],
        row['NUMTER'], row['NOMTER'], row['NUMERO_SDQS'], row['TIPO_DOCUMENTO'], row['TEMA'],
        row['TIPO_AFECTACION'], row['MEDIO_ENVIO'], convertir_fecha(row['FECHA_VENCIMIENTO']),
        row['RESPUESTA_PARCIAL'], convertir_fecha(row['FECHA_RESP_PARCIAL']), row.get('COMUNICACION_AVISO_PARCIAL'),
        convertir_fecha(row.get('FECHA_PUBLICACION_AVISO_PARCIAL')), convertir_fecha(row.get('FECHA_DESFIJACION_PARCIAL')),
        row.get('RESPUESTA_FINAL'), convertir_fecha(row.get('FECHA_RESPUESTA')), row.get('COMUNICADO_AVISO_FINAL'),
        convertir_fecha(row.get('FECHA_PUBLICACION_AVISO_FINAL')), convertir_fecha(row.get('FECHA_DESFIJACION_AVISO_FINAL')),
        convertir_fecha(row.get('FECHA_FINALIZACION_PROCESO')), row['RESPUESTA_OPORTUNA'], row['COMENTARIO'],
        row.get('DEPENDENCIA_RESPONSABLE'), row['ESTADO'], row['VENCIDO']
    ))

# Confirmar la transacción y cerrar la conexión
conn.commit()
cur.close()
conn.close()
