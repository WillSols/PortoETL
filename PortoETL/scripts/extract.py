import requests
import pandas as pd
from bs4 import BeautifulSoup

def fetch_data(port_name):
    if port_name == "paranagua":
        url = 'https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo'
        desired_columns = ['Chegada', 'Embarcação', 'Mercadoria', 'Saldo Total', 'Sentido']
        table_class = 'table table-bordered table-striped table-hover'
        csv_filename = 'Data/Bronze/lineup_paranagua.csv'
    elif port_name == "santos":
        url = 'https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/'
        desired_columns = ['NavioShip', 'OperaçOperat', 'BandeiraFlag', 'Cheg/Arrivald/m/y', 'MercadoriaGoods', 'PesoWeight']
        table_class = None
        table_id = 'esperados'
        csv_filename = 'Data/Bronze/lineup_santos.csv'
    else:
        print("Nome do porto não reconhecido")
        return

    response = requests.get(url, verify=(port_name != "santos"))  # Verificação apenas para paranaguá em santos dá erro.

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        all_data = []

        if table_class:
            tables = soup.find_all('table', class_=table_class)
        else:
            tables = soup.find_all('table', id=table_id)

        #Vai iterar pelas tabelas encontradas na página
        for table in tables:
            rows = table.find_all('tr')
            headers = rows[1].find_all('th') 
            column_indexes = {}

            #Criar um index com os headers desejados
            for index, header in enumerate(headers):
                header_text = header.get_text(strip=True)
                if header_text in desired_columns:
                    column_indexes[header_text] = index

            #Usar o index anterior para capturar as linhas de cada uma das tabelas
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) >= len(headers):
                    row_data = {col_name: cols[column_indexes[col_name]].get_text(strip=True) 
                                for col_name in desired_columns if col_name in column_indexes}
                    all_data.append(row_data)

        if all_data:
            df = pd.DataFrame(all_data)
            df.to_csv(csv_filename, index=False, encoding='utf-8')
            print(f"Dados salvos com sucesso em '{csv_filename}' para o porto de {port_name}.")
        else:
            print(f"Nenhuma tabela com as colunas desejadas foi encontrada para o porto de {port_name}.")
    else:
        print(f"Falha ao acessar a URL. Status Code: {response.status_code}")

if __name__ == '__main__':
    fetch_data("paranagua")
    fetch_data("santos")
