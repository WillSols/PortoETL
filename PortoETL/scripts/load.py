import pandas as pd
import os
from datetime import datetime

SILVER_COMBINED = 'Data/Silver/combined_data.csv'
GOLD_ENRICHED = 'Data/Gold/enriched_data.csv'
HISTORY_FOLDER = 'Data/History/'

def load_data(file_path):
    return pd.read_csv(file_path)

def enrich_data(df):
    #Ajusta o Arrival Date
    df.loc[df['Port'] == 'PARANAGUA', 'Arrival Date'] = df['Arrival Date'].str.split().str[0]
    #Remove o espaço depois do número, vírgula e ponto do volume de paranaguá antes de tranformar em numérico.
    df.loc[df['Port'] == 'PARANAGUA', 'Volume'] = df['Volume'].str.split().str[0].str.replace(',', '', regex=False).str.replace('.', '', regex=False)

    df['Arrival Date'] = pd.to_datetime(df['Arrival Date'], format='%d/%m/%Y', errors='coerce')
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
    
    df = df.dropna(subset=['Arrival Date'])
    return df

def save_enriched_data(df, file_path):
    df.to_csv(file_path, index=False)
    print(f"Dados enriquecidos salvos em '{file_path}'.")

def get_existing_data(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return pd.DataFrame()

def update_data(new_data, existing_data, file_path):
    combined_data = pd.concat([existing_data, new_data]).drop_duplicates(subset=['Ship', 'Arrival Date', 'Port'], keep='last')
    combined_data.to_csv(file_path, index=False)
    print(f"Dados combinados e atualizados salvos em '{file_path}'.")

def save_historical_data(df):
    if not os.path.exists(HISTORY_FOLDER):
        os.makedirs(HISTORY_FOLDER)
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    history_file = os.path.join(HISTORY_FOLDER, f'historico_{current_date}.csv')
    df.to_csv(history_file, index=False)
    print(f"Histórico salvo em '{history_file}'.")

def main():
    existing_silver_data = get_existing_data(SILVER_COMBINED)
    df_combined = load_data(SILVER_COMBINED)
    df_enriched = enrich_data(df_combined)
    update_data(df_enriched, existing_silver_data, SILVER_COMBINED)
    save_enriched_data(df_enriched, GOLD_ENRICHED)
    save_historical_data(df_enriched)

if __name__ == "__main__":
    main()
