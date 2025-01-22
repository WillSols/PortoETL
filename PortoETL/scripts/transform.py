import pandas as pd

BRONZE_PARANAGUA = 'Data/Bronze/lineup_paranagua.csv'
BRONZE_SANTOS = 'Data/Bronze/lineup_santos.csv'
SILVER_COMBINED = 'Data/Silver/combined_data.csv'

def load_data(file_path):
    return pd.read_csv(file_path)

def add_port_column(df, port_name):
    df['Port'] = port_name.upper()
    return df

def standardize_columns(df, port):
    rename_map = {
        'paranagua': {
            'Embarcação': 'Ship',
            'Mercadoria': 'Product',
            'Sentido': 'Direction',
            'Saldo Total': 'Volume',
            'Chegada': 'Arrival Date'
        },
        'santos': {
            'NavioShip': 'Ship',
            'MercadoriaGoods': 'Product',
            'BandeiraFlag': 'Nationality',
            'OperaçOperat': 'Operation',
            'PesoWeight': 'Volume',
            'Cheg/Arrivald/m/y': 'Arrival Date'
        }
    }
    
    df.rename(columns=rename_map[port], inplace=True)

    if port == 'santos':
        df['Direction'] = df.apply(determine_direction, axis=1)
        df.drop(columns=['Operation', 'Nationality'], inplace=True)
    
    return df

#Determina a direção para o porto de Santos (Usei a nacionalidade em conjunto com a operação descrita, talvez não seja a melhor forma mas foi a única disponível).
def determine_direction(row):
    if row['Operation'] == 'EMB' and row['Nationality'] != 'Brasileira':
        return 'EXP'
    elif row['Operation'] == 'EMB' and row['Nationality'] == 'Brasileira':
        return 'IMP'
    elif row['Operation'] == 'DESC' and row['Nationality'] != 'Brasileira':
        return 'IMP'
    else:
        return 'Other'

def save_combined_data(df, file_path):
    df.to_csv(file_path, index=False)

def main():
    df_paranagua = load_data(BRONZE_PARANAGUA)
    df_santos = load_data(BRONZE_SANTOS)
    
    df_paranagua = add_port_column(df_paranagua, 'paranagua')
    df_santos = add_port_column(df_santos, 'santos')
    
    df_paranagua = standardize_columns(df_paranagua, 'paranagua')
    df_santos = standardize_columns(df_santos, 'santos')
    
    df_combined = pd.concat([df_paranagua, df_santos])
    
    save_combined_data(df_combined, SILVER_COMBINED)
    print("Tabelas combinadas com sucesso!")

if __name__ == "__main__":
    main()
