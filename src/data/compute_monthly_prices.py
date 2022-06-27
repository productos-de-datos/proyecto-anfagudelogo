def compute_monthly_prices(path):
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional



    """
    # raise NotImplementedError("Implementar esta función")

    import pandas as pd

    df = pd.read_csv(path)
    temp = df.copy()
    temp["ano_mes"] = temp["fecha"].map(lambda x: x[:7])
    temp = temp.groupby("ano_mes", as_index=False)["precio"].mean()
    temp["ano_mes"] = temp["ano_mes"].map(lambda x: x + "-01")
    temp.rename(columns={"ano_mes": "fecha"}, inplace=True)
    return temp


if __name__ == "__main__":
    import doctest

    # compute_monthly_prices(df)
    doctest.testmod()
