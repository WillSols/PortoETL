import os
import sys
import schedule
import time

sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

from extract import fetch_data
from transform import main as transform_main
from load import main as load_main

#Main
def main():
    #Extrair os dados dos portos "PARANAGUA" e "SANTOS"
    print("Iniciando extração de dados...")
    fetch_data("paranagua")
    fetch_data("santos")
    
    #Transformar os dados extraídos (processamento)
    print("Iniciando transformação dos dados...")
    transform_main()
    
    #Carregar os dados enriquecidos para o arquivo final
    print("Iniciando carregamento dos dados enriquecidos...")
    load_main()
    
    print("Processo concluído com sucesso!")

#Processo de agendamento
def run_etl():
    print("Iniciando o processo de ETL diário...")
    main()
    print("Processo de ETL diário finalizado com sucesso!")

schedule.every().day.at("23:09").do(run_etl)
while True:
    schedule.run_pending()
    time.sleep(60)

if __name__ == "__main__":
    run_etl()
