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

    
    years = [val for val in range(1995,2016,1)]+[val for val in range(2018,2022,1)]
    for i in years:

        output_directory = '../data_lake/landing/{}.xlsx'.format(i)
        site_url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xlsx?raw=true'.format(i)
        wget.download(site_url,out=output_directory)

    years = [2016,2017]
    for i in years:

        output_directory = '../data_lake/landing/{}.xls'.format(i)
        site_url = 'https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/{}.xls?raw=true'.format(i)
        wget.download(site_url,out=output_directory)


    # https://github.com/jdvelasq/datalabs/blob/master/datasets/precio_bolsa_nacional/xls/2021.xlsx?raw=true

    # raise NotImplementedError("Implementar esta función")
    # print(years)

if __name__ == "__main__":
    import doctest
    ingest_data()
    doctest.testmod()
