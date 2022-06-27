def clean_data():
    """Realice la limpieza y transformación de los archivos CSV.

    Usando los archivos data_lake/raw/*.csv, cree el archivo data_lake/cleansed/precios-horarios.csv.
    Las columnas de este archivo son:

    * fecha: fecha en formato YYYY-MM-DD
    * hora: hora en formato HH
    * precio: precio de la electricidad en la bolsa nacional

    Este archivo contiene toda la información del 1997 a 2021.


    """
    # raise NotImplementedError("Implementar esta función")
    import pandas as pd
    from create_data_lake import get_project_root

    project_path = str(get_project_root())

    df_acum = pd.DataFrame()

    for i in range(1997, 2022, 1):
        df = None
        df = pd.read_csv(project_path + "/data_lake/raw/{}.csv".format(i))
        temp = pd.melt(df, id_vars=["fecha"], var_name="hora", value_name="precio")
        df_acum = pd.concat([df_acum, temp], axis=0)

    return df_acum


if __name__ == "__main__":
    import doctest

    clean_data()
    doctest.testmod()
