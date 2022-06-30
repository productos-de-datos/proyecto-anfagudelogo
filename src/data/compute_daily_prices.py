"transform clean data grouping by day"

import pandas as pd
from create_data_lake import get_project_root


def compute_daily_prices_pipe(dataframe):
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional


    """

    temporal_file = dataframe.copy()
    temporal_file = temporal_file.groupby("fecha", as_index=False)["precio"].mean()
    return temporal_file


def compute_daily_prices():
    """Compute los precios promedios diarios.

    Usando el archivo data_lake/cleansed/precios-horarios.csv, compute el prcio
    promedio diario (sobre las 24 horas del dia) para cada uno de los dias. Las
    columnas del archivo data_lake/business/precios-diarios.csv son:

    * fecha: fecha en formato YYYY-MM-DD

    * precio: precio promedio diario de la electricidad en la bolsa nacional


    """

    project_path = str(get_project_root())

    temporal_file = pd.read_csv(
        project_path + "/data_lake/cleansed/precios-horarios.csv"
    )
    temporal_file = temporal_file.groupby("fecha", as_index=False)["precio"].mean()
    temporal_file.to_csv(
        project_path + "/data_lake/business/precios-diarios.csv", index=False
    )


# raise NotImplementedError("Implementar esta función")

if __name__ == "__main__":
    import doctest

    compute_daily_prices()
    doctest.testmod()
# else:
# import doctest

# # compute_daily_prices_pipe()
# doctest.testmod()
