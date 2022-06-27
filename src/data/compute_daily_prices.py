def compute_daily_prices(df):
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional


    """
    import pandas as pd

    temp = df.copy()
    temp = temp.groupby("fecha", as_index=False)["precio"].mean()
    return temp
    # .to_csv(relative_path+'\\data_lake\\business\\precios-diarios.csv', index=False)


# raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    # compute_daily_prices()
    doctest.testmod()
