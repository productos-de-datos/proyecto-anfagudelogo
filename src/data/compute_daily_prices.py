def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional



    """
    import pandas as pd

    relative_path = '\\'.join(__file__.split('\\')[:-2])
    df = pd.read_csv(relative_path+'\\data_lake\\cleansed\\precios-horarios.csv')
    temp = df.copy()
    temp = temp.groupby('fecha',as_index = False)['precio'].mean()
    temp.to_csv(relative_path+'\\data_lake\\business\\precios-diarios.csv', index=False) 
    
    
    print(temp.head()) 



    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest
    compute_daily_prices()
    doctest.testmod()
