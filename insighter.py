import pandas as pd
from itertools import combinations
from statistics import median
import os

# Leer los datos desde el archivo CSV sin encabezados
df = pd.read_csv('data.csv', header=None, names=['IdSorteo', 'N1', 'N2', 'N3', 'N4', 'N5'])

# Función para calcular sorteos transcurridos entre apariciones considerando todas las columnas N1-N5
def calcular_intervalos(df, columnas):
    resultados = []
    
    # Unificar todas las columnas de números en una sola columna
    df_melted = df.melt(id_vars=['IdSorteo'], value_vars=columnas, var_name='Grupo', value_name='Numero')
    
    # Eliminar filas con valores NaN y ordenar por IdSorteo
    df_melted = df_melted.dropna().sort_values(by=['Numero', 'IdSorteo'])

    # Calcular intervalos para cada número
    for num in df_melted['Numero'].unique():
        subset = df_melted[df_melted['Numero'] == num]
        apariciones = subset['IdSorteo'].values
        sorteos = df['IdSorteo'].unique()

        if len(apariciones) > 0:
            segmentos = [apariciones[0] - sorteos[0]] + [apariciones[i] - apariciones[i-1] for i in range(1, len(apariciones))]
            if apariciones[-1] < sorteos[-1]:
                segmentos.append(sorteos[-1] - apariciones[-1])
            promedio = sum(segmentos) / len(segmentos)
            mediana = median(segmentos)
            ultimos_sorteos = sorteos[-1] - apariciones[-1]
            
            # Calcular la probabilidad respecto al promedio
            if ultimos_sorteos <= promedio:
                probabilidad_avg = ultimos_sorteos / promedio
            else:
                probabilidad_avg = promedio / ultimos_sorteos
            
            # Calcular la probabilidad respecto a la mediana
            if ultimos_sorteos <= mediana:
                probabilidad_mediana = ultimos_sorteos / mediana
            else:
                probabilidad_mediana = mediana / ultimos_sorteos
            
            # Calcular la probabilidad fusión (promedio de las dos probabilidades anteriores)
            probabilidad_fusion = (probabilidad_avg + probabilidad_mediana) / 2

            resultados.append((num, segmentos, promedio, mediana, ultimos_sorteos, probabilidad_avg, probabilidad_mediana, probabilidad_fusion))

    return pd.DataFrame(resultados, columns=['Numero', 'Intervalos', 'Promedio', 'Mediana', 'UltimosSorteos', 'Probabilidad_Avg', 'Probabilidad_Mediana', 'Probabilidad_Fusion'])

# Función para generar prospectos con peso basado en una métrica
def generar_prospectos_con_peso(resultados_numeros, metrica, metodo_calculo):
    # Filtrar números con probabilidad superior a 0.65
    numeros_prospecto = resultados_numeros[resultados_numeros[metrica] > 0.65]
    
    # Ordenar por probabilidad de mayor a menor
    numeros_prospecto = numeros_prospecto.sort_values(by=metrica, ascending=False)

    # Generar combinaciones de números
    combinaciones = []
    for comb in combinations(numeros_prospecto['Numero'], 5):  # Combinaciones de 5 números
        comb_ordenada = sorted(comb)  # Ordenar cada combinación de números de menor a mayor
        probabilidad_comb = numeros_prospecto[numeros_prospecto['Numero'].isin(comb_ordenada)][metrica].sum()
        combinaciones.append((*comb_ordenada, probabilidad_comb, metodo_calculo))
    
    # Convertir a DataFrame y añadir la columna del método de cálculo
    df_combinaciones = pd.DataFrame(combinaciones, columns=['N1', 'N2', 'N3', 'N4', 'N5', 'Peso', 'MetodoCalculo'])
    df_combinaciones = df_combinaciones.sort_values(by='Peso', ascending=False)
    
    return df_combinaciones

# Función para guardar prospectos
def guardar_prospectos(df_prospectos, metodo_calculo, id_sorteo):
    filename = f'prospectos_{id_sorteo}_{metodo_calculo}.csv'
    if os.path.exists(filename):
        df_existente = pd.read_csv(filename)
        df_combined = pd.concat([df_existente, df_prospectos]).drop_duplicates().reset_index(drop=True)
    else:
        df_combined = df_prospectos
    
    df_combined.to_csv(filename, index=False)
    print(f'Prospectos guardados en {filename}')

# Función para cargar prospectos
def cargar_prospectos(metodo_calculo, id_sorteo):
    filename = f'prospectos_{id_sorteo}_{metodo_calculo}.csv'
    try:
        df_prospectos = pd.read_csv(filename)
        print(f'Prospectos cargados desde {filename}')
        return df_prospectos
    except FileNotFoundError:
        print(f'No se encontraron prospectos previos para {metodo_calculo}')
        return pd.DataFrame()

# Función para comparar los prospectos generados contra los resultados reales del siguiente sorteo
def comparar_prospectos_con_resultados(df_sorteo, df_prospectos, lastdraft):
    aciertos = 0
    numeros_acertados = []
    numeros_ = ()
    numeros_sorteo = ()

    # Consolidar todos los números de los prospectos
    numeros_prospecto = set(df_prospectos[['N1', 'N2', 'N3', 'N4', 'N5']].values.flatten())
    
    if lastdraft == 0:
        # Comparar con los números del sorteo actual
        numeros_sorteo = set(df_sorteo[['N1', 'N2', 'N3', 'N4', 'N5']])

        # Identificar los números acertados
        aciertos_numeros = numeros_sorteo & numeros_prospecto
        aciertos += len(aciertos_numeros)
        numeros_acertados = list(aciertos_numeros)

    return {
        'IdSorteo': lastdraft if lastdraft > 0 else df_sorteo['IdSorteo'],
        'Numeros_Sorteo':  numeros_sorteo if numeros_sorteo else () ,
        'Numeros_Prospecto': numeros_prospecto, 
        'Aciertos': aciertos,
        'Numeros_Acertados': numeros_acertados if numeros_acertados else [],
    }

# Proceso iterativo de generación de prospectos y comparación
def proceso_completo(df_original):
    resultados_comparacion = []

    # Ordenar por IdSorteo para procesar secuencialmente
    df_sorteos_ordenados = df_original.sort_values(by='IdSorteo')

    # Iterar desde el segundo sorteo para poder comparar con el anterior
    for idx in range(10, len(df_sorteos_ordenados) + 1):
        # Obtener los sorteos hasta el sorteo anterior
        df_previos = df_sorteos_ordenados.iloc[:idx]
        id_sorteo = len(df_sorteos_ordenados) + 1
        if idx < len(df_sorteos_ordenados):
            id_sorteo = df_sorteos_ordenados.iloc[idx]['IdSorteo']
        print(id_sorteo)
        
        # Calcular intervalos y generar prospectos basados en los sorteos previos
        resultados_numeros = calcular_intervalos(df_previos, ['N1', 'N2', 'N3', 'N4', 'N5'])

        # Cargar prospectos guardados previamente o generar nuevos si no existen
        df_prospectos_fusion = cargar_prospectos('Fusión', id_sorteo)
        df_prospectos_promedio = cargar_prospectos('Promedio', id_sorteo)
        df_prospectos_mediana = cargar_prospectos('Mediana', id_sorteo)

        if df_prospectos_fusion.empty:
            df_prospectos_fusion = generar_prospectos_con_peso(resultados_numeros, 'Probabilidad_Fusion', 'Fusión')
            guardar_prospectos(df_prospectos_fusion, 'Fusión', id_sorteo)

        if df_prospectos_promedio.empty:
            df_prospectos_promedio = generar_prospectos_con_peso(resultados_numeros, 'Probabilidad_Avg', 'Promedio')
            guardar_prospectos(df_prospectos_promedio, 'Promedio', id_sorteo)

        if df_prospectos_mediana.empty:
            df_prospectos_mediana = generar_prospectos_con_peso(resultados_numeros, 'Probabilidad_Mediana', 'Mediana')
            guardar_prospectos(df_prospectos_mediana, 'Mediana', id_sorteo)

        # Combinar los prospectos generados
        df_prospectos = pd.concat([df_prospectos_fusion, df_prospectos_promedio, df_prospectos_mediana], ignore_index=True)

        if idx < len(df_sorteos_ordenados):
            # Obtener los resultados reales del siguiente sorteo
            siguiente_sorteo = df_sorteos_ordenados.iloc[idx]
            resultado_comparacion = comparar_prospectos_con_resultados(siguiente_sorteo, df_prospectos, 0)
            resultados_comparacion.append(resultado_comparacion)
        else:
            # Comparar los prospectos con los resultados reales
            resultado_comparacion = comparar_prospectos_con_resultados(siguiente_sorteo, df_prospectos, id_sorteo)
            resultados_comparacion.append(resultado_comparacion)

    return pd.DataFrame(resultados_comparacion)

# Llamada a la función para ejecutar el proceso completo
df_resultados_comparacion = proceso_completo(df)
df_resultados_comparacion.to_csv('resultados_comparacion.csv', index=False)