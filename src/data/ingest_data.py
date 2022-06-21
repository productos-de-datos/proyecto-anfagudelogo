"""
Módulo de ingestión de datos.
-------------------------------------------------------------------------------

"""

import pandas as pd


def ingest_data():
    """Ingeste los datos externos a la capa landing del data lake.

    Del repositorio jdvelasq/datalabs/precio_bolsa_nacional/xls/ descarge los
    archivos de precios de bolsa nacional en formato xls a la capa landing. La
    descarga debe realizarse usando únicamente funciones de Python.

    """
    import wget
    import glob
    import os

    relative_path = "\\".join(__file__.split("\\")[:-2])
    output_directory = os.path.join(relative_path, "data_lake\\landing")
    years_xlsx = [val for val in range(1995, 2016, 1)] + [
        val for val in range(2018, 2022, 1)
    ]
    years_xls = [2016, 2017]

    for i in years_xlsx:

        site_url = "https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xlsx?raw=true".format(
            i
        )
        wget.download(site_url, out=output_directory)

    for i in years_xls:

        site_url = "https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xls?raw=true".format(
            i
        )
        wget.download(site_url, out=output_directory)

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    # ingest_data()
    doctest.testmod()
