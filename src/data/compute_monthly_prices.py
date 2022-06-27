def compute_monthly_prices_pipe(df):
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional



    """
    # raise NotImplementedError("Implementar esta función")

    import pandas as pd

    temp = df.copy()
    temp["ano_mes"] = temp["fecha"].map(lambda x: x[:7])
    temp = temp.groupby("ano_mes", as_index=False)["precio"].mean()
    temp["ano_mes"] = temp["ano_mes"].map(lambda x: x + "-01")
    temp.rename(columns={"ano_mes": "fecha"}, inplace=True)
    return temp


def compute_monthly_prices():
    """Compute los precios promedios mensuales.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio mensual. Las
    columnas del archivo data_lake/business/precios-mensuales.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio mensual de la electricidad en la bolsa nacional



    """
    # raise NotImplementedError("Implementar esta función")

    import pandas as pd
    from create_data_lake import get_project_root

    project_path = str(get_project_root())

    temp = pd.read_csv(project_path + "/data_lake/cleansed/precios-horarios.csv")
    temp["ano_mes"] = temp["fecha"].map(lambda x: x[:7])
    temp = temp.groupby("ano_mes", as_index=False)["precio"].mean()
    temp["ano_mes"] = temp["ano_mes"].map(lambda x: x + "-01")
    temp.rename(columns={"ano_mes": "fecha"}, inplace=True)
    temp.to_csv(project_path + "/data_lake/business/precios-mensuales.csv", index=False)


if __name__ == "__main__":
    import doctest

    compute_monthly_prices()
    doctest.testmod()
# else:
#     import doctest

# compute_monthly_prices_pipe()
# doctest.testmod()
