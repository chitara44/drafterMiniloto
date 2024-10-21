import pandas as pd
from itertools import combinations
from statistics import median
import os
import subprocess
import shutil
import sys
import requests
from bs4 import BeautifulSoup
import re


class SorteoProcessor:
    def __init__(self, csv_path='data.csv'):
        self.csv_path = csv_path
        self.df = None

    def cargar_datos(self):
        self.df = pd.read_csv(self.csv_path, header=None, names=['IdSorteo', 'N1', 'N2', 'N3', 'N4', 'N5'])

    def calcular_intervalos(self, columnas):
        resultados = []
        df_melted = self.df.melt(id_vars=['IdSorteo'], value_vars=columnas, var_name='Grupo', value_name='Numero')
        df_melted = df_melted.dropna().sort_values(by=['Numero', 'IdSorteo'])
        
        for num in df_melted['Numero'].unique():
            subset = df_melted[df_melted['Numero'] == num]
            apariciones = subset['IdSorteo'].values
            sorteos = self.df['IdSorteo'].unique()

            if len(apariciones) > 0:
                segmentos = [apariciones[0] - sorteos[0]] + [apariciones[i] - apariciones[i-1] for i in range(1, len(apariciones))]
                if apariciones[-1] < sorteos[-1]:
                    segmentos.append(sorteos[-1] - apariciones[-1])
                promedio = sum(segmentos) / len(segmentos)
                mediana = median(segmentos)
                ultimos_sorteos = sorteos[-1] - apariciones[-1]
                
                probabilidad_avg = min(ultimos_sorteos / promedio, promedio / ultimos_sorteos)
                probabilidad_mediana = min(ultimos_sorteos / mediana, mediana / ultimos_sorteos)
                probabilidad_fusion = (probabilidad_avg + probabilidad_mediana) / 2

                resultados.append((num, segmentos, promedio, mediana, ultimos_sorteos, probabilidad_avg, probabilidad_mediana, probabilidad_fusion))

        return pd.DataFrame(resultados, columns=['Numero', 'Intervalos', 'Promedio', 'Mediana', 'UltimosSorteos', 'Probabilidad_Avg', 'Probabilidad_Mediana', 'Probabilidad_Fusion'])

    def generar_prospectos_con_peso(self, resultados_numeros, metrica, metodo_calculo):
        numeros_prospecto = resultados_numeros[resultados_numeros[metrica] > 0.65].sort_values(by=metrica, ascending=False)
        combinaciones = [(*sorted(comb), numeros_prospecto[numeros_prospecto['Numero'].isin(comb)][metrica].sum(), metodo_calculo) 
                         for comb in combinations(numeros_prospecto['Numero'], 5)]
        return pd.DataFrame(combinaciones, columns=['N1', 'N2', 'N3', 'N4', 'N5', 'Peso', 'MetodoCalculo'])

    def guardar_prospectos(self, df_prospectos, metodo_calculo, id_sorteo):
        filename = f'prospectos_{id_sorteo}_{metodo_calculo}.csv'
        if os.path.exists(filename):
            df_existente = pd.read_csv(filename)
            df_combined = pd.concat([df_existente, df_prospectos]).drop_duplicates().reset_index(drop=True)
        else:
            df_combined = df_prospectos
        df_combined.to_csv(filename, index=False)

    def cargar_prospectos(self, metodo_calculo, id_sorteo):
        filename = f'prospectos_{id_sorteo}_{metodo_calculo}.csv'
        try:
            return pd.read_csv(filename)
        except FileNotFoundError:
            return pd.DataFrame()

    def comparar_prospectos_con_resultados(self, df_sorteo, df_prospectos, lastdraft):
        numeros_prospecto = set(df_prospectos[['N1', 'N2', 'N3', 'N4', 'N5']].values.flatten())
        if lastdraft == 0:
            numeros_sorteo = set(df_sorteo[['N1', 'N2', 'N3', 'N4', 'N5']])
            aciertos_numeros = numeros_sorteo & numeros_prospecto
            return {
                'IdSorteo': lastdraft if lastdraft > 0 else df_sorteo['IdSorteo'],
                'Numeros_Sorteo': numeros_sorteo,
                'Numeros_Prospecto': numeros_prospecto,
                'Aciertos': len(aciertos_numeros),
                'Numeros_Acertados': list(aciertos_numeros)
            }

    def proceso_completo(self, finalDraft):
        resultados_comparacion = []
        df_sorteos_ordenados = self.df.sort_values(by='IdSorteo')
        for idx in range(10, finalDraft):
            df_previos = df_sorteos_ordenados.iloc[:idx]
            id_sorteo = df_sorteos_ordenados.iloc[idx]['IdSorteo']
            resultados_numeros = self.calcular_intervalos(['N1', 'N2', 'N3', 'N4', 'N5'])
            
            df_prospectos_fusion = self.cargar_prospectos('Fusión', id_sorteo)
            if df_prospectos_fusion.empty:
                df_prospectos_fusion = self.generar_prospectos_con_peso(resultados_numeros, 'Probabilidad_Fusion', 'Fusión')
                self.guardar_prospectos(df_prospectos_fusion, 'Fusión', id_sorteo)

            df_prospectos = pd.concat([df_prospectos_fusion], ignore_index=True)
            siguiente_sorteo = df_sorteos_ordenados.iloc[idx]
            resultados_comparacion.append(self.comparar_prospectos_con_resultados(siguiente_sorteo, df_prospectos, 0))

        return pd.DataFrame(resultados_comparacion)


### Clase para realizar scraping de sorteos
class SorteoScraper:
    def request_sorteo(self, sorteo):
        url = f'https://www.baloto.com/miloto/resultados-miloto/{sorteo}'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser") if response.status_code == 200 else ""
        return soup, response.status_code

    def extraer_numeros_sorteo(self, soup_text):
        div_container2 = soup_text.find("div", class_="row d-flex justify-content-center")
        texto = div_container2.get_text(separator=' ', strip=True)
        return ','.join(re.findall(r'\b\d{2}\b', texto))

    def save_draftline(self, filename, draftline):
        with open(filename, 'a') as file:
            file.write(draftline + '\n')

    def run_scraper(self, initial, final):
        for i in range(initial, final):
            soup_text, response_code = self.request_sorteo(i)
            if response_code == 200:
                numeros = self.extraer_numeros_sorteo(soup_text)
                trDraftComplete = f'{i},{numeros}'
                current_directory = os.path.dirname(os.path.abspath(__file__))
                self.save_draftline(current_directory + '\\data.csv', trDraftComplete)
            else:
                print(f"Sorteo {i} no encontrado.")

try:
    # initialDraft = int(sys.argv[1])
    # finalDraft = int(sys.argv[2])
    # omitirScrapping = str(sys.argv[3])
    
    initialDraft = int(1)
    finalDraft = int(206)
    omitirScrapping = 'Y'
    if  omitirScrapping != 'Y':
        scraper = SorteoScraper()
        scraper.run_scraper(initialDraft, finalDraft)
    
    processor = SorteoProcessor()
    processor.cargar_datos()
    df_resultados_comparacion = processor.proceso_completo(finalDraft)
    df_resultados_comparacion.to_csv('comparing_results.csv', index=False)

except Exception as e:
    print(f"Ocurrió un error: {e}")

