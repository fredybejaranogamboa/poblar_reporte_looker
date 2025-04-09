import mysql.connector
import pandas as pd
from config import db_config

# Conexión a la base de datos MySQL
conn = mysql.connector.connect(**db_config)
cur = conn.cursor()

# Cargar el archivo Excel
data = pd.read_excel('Dashboard por actividad PQRS.xlsx')

# # Omitir filas donde la primera columna no es un número válido
# data = data[pd.to_numeric(data.iloc[:, 0], errors='coerce').notnull()]

# # Reemplazar NaN con None
# data = data.where(pd.notnull(data), None)

# Preparar y ejecutar las consultas de inserción o actualización
for index, row in data.iterrows():
    # print(f"Procesando fila {index + 1}: {row.to_dict()}")
    
    # Verificar si NUMPRO ya existe en la tabla
    cur.execute("SELECT * FROM pqrsf_actividad WHERE nomact = %s", (row['nomact'],))
    existing_row = cur.fetchone()
    
    # print(f"Resultado de la consulta: {existing_row}")
    
    if existing_row:
        # Comparar los datos existentes con los nuevos datos
        existing_data = {
            'nomact' : existing_row[1],
            'total_dias': existing_row[2],
            'numpro_cnt': existing_row[3],
            'avg_dias': existing_row[4],
            'max_dias': existing_row[5],
            'min_dias': existing_row[6],
            'rango_dias': existing_row[7],
            'varianza': existing_row[8],
            'desv_std': existing_row[9],
            'moda': existing_row[10],
            'numpro_max_dias': existing_row[11]
        }
        
        new_data = {
            'nomact': row['nomact'],
            'total_dias': row['total_dias'],
            'numpro_cnt': row['numpro_cnt'],
            'avg_dias': row['avg_dias'],
            'max_dias': row['max_dias'],
            'min_dias': row['min_dias'],
            'rango_dias': row['rango_dias'],
            'varianza': row['varianza'],
            'desv_std': row['desv_std'],
            'moda': row['moda'],
            'numpro_max_dias': row['numpro_max_dias'],
        }
        
        print(f"Datos existentes: {existing_data}")
        print(f"Nuevos datos: {new_data}")
        
        if existing_data != new_data:
            
            print(f"Actualizando los datos para nomact: {row['nomact']}")
            
            # Actualizar los datos si han cambiado
            cur.execute("""
                UPDATE pqrsf_actividad SET
                    nomact = %s, total_dias = %s, numpro_cnt = %s, avg_dias = %s, max_dias = %s,
                    min_dias = %s, rango_dias = %s, varianza = %s, desv_std = %s, moda = %s,
                    numpro_max_dias = %s
                WHERE nomact = %s
            """, (
                new_data['nomact'], new_data['total_dias'], new_data['numpro_cnt'], new_data['avg_dias'], new_data['max_dias'],
                new_data['min_dias'], new_data['rango_dias'], new_data['varianza'], new_data['desv_std'], new_data['moda'],
                new_data['numpro_max_dias']
            ))
            # Confirmar la transacción después de cada actualización
            conn.commit()
        else:
            
            print(f"Insertando nuevos datos para nomact: {row['nomact']}")
            
            # Insertar los datos si no existen
            cur.execute("""
                INSERT INTO pqrsf_actividad (nomact, total_dias, numpro_cnt, avg_dias, max_dias, min_dias, rango_dias, varianza, desv_std, moda, numpro_max_dias)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                new_data['nomact'], new_data['total_dias'], new_data['numpro_cnt'], new_data['avg_dias'], new_data['max_dias'],
                new_data['min_dias'], new_data['rango_dias'], new_data['varianza'], new_data['desv_std'], new_data['moda'],
                new_data['numpro_max_dias']     
            ))
            # Confirmar la transacción después de cada inserción
            conn.commit()

# Cerrar la conexión
cur.close()
conn.close()
