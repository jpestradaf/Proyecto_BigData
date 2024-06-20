import geopandas as gpd
import pandas as pd
import random
from shapely.geometry import Point
import datetime
import socket
import json

# Paths de los archivos
gdf_path = '50001.parquet'
customers_path = 'customers.parquet'
employees_path = 'employees.parquet'
neighborhood_path ='medellin_neighborhoods.parquet'

# Leer datos
def cargar_datos():
    gdf_medellin = gpd.read_parquet(gdf_path)
    df_customers = pd.read_parquet(customers_path)
    df_employees = pd.read_parquet(employees_path)
    gdf_neighborhood = gpd.read_parquet(neighborhood_path)
    return gdf_medellin, df_customers, df_employees, gdf_neighborhood

# Generar coordenadas aleatorias dentro de Medellín
def generar_coordenadas(poligono,gdf_neighborhood):
    min_x, min_y, max_x, max_y = poligono.bounds
    while True:
        punto_aleatorio = Point(random.uniform(min_x, max_x), random.uniform(min_y, max_y))
        if poligono.contains(punto_aleatorio):
            punto = Point(punto_aleatorio.x, punto_aleatorio.y)
            filtro = gdf_neighborhood['geometry'].geometry.contains(punto)
            poligono_encontrado = gdf_neighborhood[filtro]
            if not poligono_encontrado.empty:
                return punto_aleatorio.x, punto_aleatorio.y

#Generar el barrio y la comuna donde estan las coordendas         
def generar_comuna(longitude, latitude, gdf_neighborhood):
    punto = Point(longitude, latitude)
    filtro = gdf_neighborhood['geometry'].geometry.contains(punto)
    poligono_encontrado = gdf_neighborhood[filtro]
    if not poligono_encontrado.empty:
        return poligono_encontrado.iloc[0]['OBJECTID']
    else:
        return None

# Obtener siguiente carácter en un string
def siguiente_caracter(caracter):
    valor_ascii = ord(caracter)
    siguiente_valor_ascii = valor_ascii + 1
    if valor_ascii <= ord('z') and valor_ascii >= ord('a'):
        if siguiente_valor_ascii > ord('z'):
            siguiente_valor_ascii = ord('a')
    elif valor_ascii <= ord('9') and valor_ascii >= ord('1'):
        if siguiente_valor_ascii > ord('9'):
            siguiente_valor_ascii = ord('0')
    else:
        siguiente_valor_ascii = valor_ascii
    siguiente_caracter = chr(siguiente_valor_ascii)
    return siguiente_caracter

# Incrementar código alfanumérico
def aumentar_codigo(codigo):
    lista = list(codigo)
    n = -1
    while True:
        lista[n] = siguiente_caracter(lista[n])
        if lista[n] == 'a' or lista[n] == '0' or lista[n] == '-':
            n = n - 1
            if len(lista) * -1 > n:
                break
        else:
            break
    new_codigo = ''.join(lista)
    return new_codigo

# Crear datos simulados
def crear_datos(gdf_medellin, df_customers, df_employees, gdf_neighborhood, mu, sigma, order):
    longitude, latitude = generar_coordenadas(gdf_medellin.loc[0]['geometry'], gdf_neighborhood)
    object_id = int(generar_comuna(longitude, latitude, gdf_neighborhood))
    date_actual = datetime.datetime.now()
    date_formateada = date_actual.strftime("%d/%m/%Y %H:%M:%S")
    customer_size = df_customers['customer_id'].size
    customer = random.randint(0, customer_size - 1)
    customer_id = int(df_customers.loc[customer]['customer_id'])
    employees_size = df_employees['employee_id'].size
    employees = random.randint(0, employees_size - 1)
    employees_id = int(df_employees.loc[employees]['employee_id'])
    quantity_products = int(random.gauss(mu, sigma))
    order_id = aumentar_codigo(order)
    datos = {
        "object_id": object_id,
        "latitude": latitude,
        "longitude": longitude,
        "date": date_formateada,
        "customer_id": customer_id,
        "employee_id": employees_id,
        "quantity_products": quantity_products,
        "order_id": order_id
    }
    return datos

# Configuración del socket servidor
def iniciar_servidor():
    mi_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    mi_socket.bind(('localhost', 8000))
    mi_socket.listen()
    return mi_socket

# Generar datos y enviar al cliente
def enviar_datos(socket_cliente, gdf_medellin, df_customers, df_employees, gdf_neighborhood, mu, sigma, order_base):
    list_data = []
    for _ in range(random.randint(5, 10)):  # Genera más de 5 datos
        data = crear_datos(gdf_medellin, df_customers, df_employees, gdf_neighborhood, mu, sigma, order_base)
        order_base = data['order_id']
        list_data.append(data)
    json_data = json.dumps(list_data, default=str)  # Convertir datetime a string antes de serializar
    byte_data = json_data.encode('utf-8')
    socket_cliente.send(byte_data)
    socket_cliente.close()
    return order_base

# Ejecutar servidor
def ejecutar_servidor():
    gdf_medellin, df_customers, df_employees, gdf_neighborhood = cargar_datos()
    order_base = 'd8b9b417-b098-4344-b137-362894e4dccb'
    mu, sigma = 500, 175
    mi_socket = iniciar_servidor()
    while True:
        conexion, addr = mi_socket.accept()
        order_base = enviar_datos(conexion, gdf_medellin, df_customers, df_employees, gdf_neighborhood, mu, sigma, order_base)
        print("Envio exitoso:",order_base)

# Ejecutar servidor
if __name__ == "__main__":
    ejecutar_servidor()
