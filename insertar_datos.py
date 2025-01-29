import mysql.connector
import pandas as pd
from config import db_config, file_path

# Conexión a la base de datos MySQL
conn = mysql.connector.connect(**db_config)
cur = conn.cursor()

# Cargar el archivo Excel
data = pd.read_excel(file_path)

# Omitir filas donde la primera columna no es un número válido
data = data[pd.to_numeric(data.iloc[:, 0], errors='coerce').notnull()]

# Reemplazar NaN con None
data = data.where(pd.notnull(data), None)

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

# Preparar y ejecutar las consultas de inserción o actualización
for index, row in data.iterrows():
    # Verificar si NUMPRO ya existe en la tabla
    cur.execute("SELECT * FROM pqrsf WHERE numpro = %s", (row['NUMPRO'],))
    existing_row = cur.fetchone()
    
    if existing_row:
        # Comparar los datos existentes con los nuevos datos
        existing_data = {
            'numpro': existing_row[0],
            'fecha_creacion': existing_row[1],
            'categoria': existing_row[2],
            'localidad': existing_row[3],
            'numrad': existing_row[4],
            'fecrad': existing_row[5],
            'vencimiento_dias': existing_row[6],
            'tipo_tercero': existing_row[7],
            'numter': existing_row[8],
            'nomter': existing_row[9],
            'numero_sdqs': existing_row[10],
            'tipo_documento': existing_row[11],
            'tema': existing_row[12],
            'tipo_afectacion': existing_row[13],
            'medio_envio': existing_row[14],
            'fecha_vencimiento': existing_row[15],
            'respuesta_parcial': existing_row[16],
            'fecha_resp_parcial': existing_row[17],
            'comunicacion_aviso_parcial': existing_row[18],
            'fecha_publicacion_aviso_parcial': existing_row[19],
            'fecha_desfijacion_parcial': existing_row[20],
            'respuesta_final': existing_row[21],
            'fecha_respuesta': existing_row[22],
            'comunicado_aviso_final': existing_row[23],
            'fecha_publicacion_aviso_final': existing_row[24],
            'fecha_desfijacion_aviso_final': existing_row[25],
            'fecha_finalizacion_proceso': existing_row[26],
            'respuesta_oportuna': existing_row[27],
            'comentario': existing_row[28],
            'dependencia_responsable': existing_row[29],
            'estado': existing_row[30],
            'vencido': existing_row[31]
        }
        
        new_data = {
            'numpro': row['NUMPRO'],
            'fecha_creacion': convertir_fecha(row['FECHA_CREACION']),
            'categoria': row['CATEGORIA'],
            'localidad': row['LOCALIDAD'],
            'numrad': row['NUMRAD'],
            'fecrad': convertir_fecha(row['FECRAD']),
            'vencimiento_dias': row['VENCIMIENTO_DIAS'],
            'tipo_tercero': row['TIPO_TERCERO'],
            'numter': row['NUMTER'],
            'nomter': row['NOMTER'],
            'numero_sdqs': row['NUMERO_SDQS'],
            'tipo_documento': row['TIPO_DOCUMENTO'],
            'tema': row['TEMA'],
            'tipo_afectacion': row['TIPO_AFECTACION'],
            'medio_envio': row['MEDIO_ENVIO'],
            'fecha_vencimiento': convertir_fecha(row['FECHA_VENCIMIENTO']),
            'respuesta_parcial': row['RESPUESTA_PARCIAL'],
            'fecha_resp_parcial': convertir_fecha(row['FECHA_RESP_PARCIAL']),
            'comunicacion_aviso_parcial': row.get('COMUNICACION_AVISO_PARCIAL'),
            'fecha_publicacion_aviso_parcial': convertir_fecha(row.get('FECHA_PUBLICACION_AVISO_PARCIAL')),
            'fecha_desfijacion_parcial': convertir_fecha(row.get('FECHA_DESFIJACION_PARCIAL')),
            'respuesta_final': row.get('RESPUESTA_FINAL'),
            'fecha_respuesta': convertir_fecha(row.get('FECHA_RESPUESTA')),
            'comunicado_aviso_final': row.get('COMUNICADO_AVISO_FINAL'),
            'fecha_publicacion_aviso_final': convertir_fecha(row.get('FECHA_PUBLICACION_AVISO_FINAL')),
            'fecha_desfijacion_aviso_final': convertir_fecha(row.get('FECHA_DESFIJACION_AVISO_FINAL')),
            'fecha_finalizacion_proceso': convertir_fecha(row.get('FECHA_FINALIZACION_PROCESO')),
            'respuesta_oportuna': row['RESPUESTA_OPORTUNA'],
            'comentario': row['COMENTARIO'],
            'dependencia_responsable': row.get('DEPENDENCIA_RESPONSABLE'),
            'estado': row['ESTADO'],
            'vencido': row.get('VENCIDO')
        }
        
        if existing_data != new_data:
            # Actualizar los datos si han cambiado
            cur.execute("""
                UPDATE pqrsf SET
                    fecha_creacion = %s, categoria = %s, localidad = %s, numrad = %s, fecrad = %s,
                    vencimiento_dias = %s, tipo_tercero = %s, numter = %s, nomter = %s, numero_sdqs = %s,
                    tipo_documento = %s, tema = %s, tipo_afectacion = %s, medio_envio = %s, fecha_vencimiento = %s,
                    respuesta_parcial = %s, fecha_resp_parcial = %s, comunicacion_aviso_parcial = %s,
                    fecha_publicacion_aviso_parcial = %s, fecha_desfijacion_parcial = %s, respuesta_final = %s,
                    fecha_respuesta = %s, comunicado_aviso_final = %s, fecha_publicacion_aviso_final = %s,
                    fecha_desfijacion_aviso_final = %s, fecha_finalizacion_proceso = %s, respuesta_oportuna = %s,
                    comentario = %s, dependencia_responsable = %s, estado = %s, vencido = %s
                WHERE numpro = %s
            """, (
                new_data['fecha_creacion'], new_data['categoria'], new_data['localidad'], new_data['numrad'], new_data['fecrad'],
                new_data['vencimiento_dias'], new_data['tipo_tercero'], new_data['numter'], new_data['nomter'], new_data['numero_sdqs'],
                new_data['tipo_documento'], new_data['tema'], new_data['tipo_afectacion'], new_data['medio_envio'], new_data['fecha_vencimiento'],
                new_data['respuesta_parcial'], new_data['fecha_resp_parcial'], new_data['comunicacion_aviso_parcial'],
                new_data['fecha_publicacion_aviso_parcial'], new_data['fecha_desfijacion_parcial'], new_data['respuesta_final'],
                new_data['fecha_respuesta'], new_data['comunicado_aviso_final'], new_data['fecha_publicacion_aviso_final'],
                new_data['fecha_desfijacion_aviso_final'], new_data['fecha_finalizacion_proceso'], new_data['respuesta_oportuna'],
                new_data['comentario'], new_data['dependencia_responsable'], new_data['estado'], new_data['vencido'],
                new_data['numpro']
            ))
            # Confirmar la transacción después de cada actualización
            conn.commit()
    else:
        # Insertar los datos si no existen
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
            row.get('DEPENDENCIA_RESPONSABLE'), row['ESTADO'], row.get('VENCIDO')
        ))
        # Confirmar la transacción después de cada inserción
        conn.commit()

# Cerrar la conexión
cur.close()
conn.close()
