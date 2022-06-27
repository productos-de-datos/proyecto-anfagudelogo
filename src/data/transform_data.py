def transform_data():
    """Transforme los archivos xls a csv.

    Transforme los archivos data_lake/landing/*.xls a data_lake/raw/*.csv. Hay
    un archivo CSV por cada archivo XLS en la capa landing. Cada archivo CSV
    tiene como columnas la fecha en formato YYYY-MM-DD y las horas H00, ...,
    H23.

    """
    import pandas as pd
    import glob
    from datetime import datetime

    relative_path = "\\".join(__file__.split("\\")[:-3])

    def clean_columns(data):
        try:
            temp = ["fecha"] + [str(col).replace(".0", "") for col in data.columns[1:]]
            data.columns = ["fecha"] + [
                "H0" + val if len(val) == 1 else "H" + val for val in temp[1:]
            ]
            return data
        except Exception as e:
            print(e)

    def clean_fecha(data):
        try:
            df = data
            df["fecha"] = df["fecha"].map(
                lambda x: datetime.strptime(str(x).strip()[:10], "%Y-%m-%d")
            )
            return df
        except Exception as e:
            print(e)

    def read_data(year):
        try:
            input_path = glob.glob(
                relative_path + "\\data_lake\\landing/{}.xl*".format(year)
            )
            df = pd.read_excel(
                input_path[0], header=None, usecols=[val for val in range(0, 25, 1)]
            )
            return df
        except Exception as e:
            print(e)

    def clean_data(data):
        try:
            df = data.copy()
            df.dropna(axis=0, how="all", inplace=True)
            if df.iloc[0, 0] != "Fecha":
                df = df.iloc[1:, :]

            df.columns = df.iloc[0, :]
            df = df.iloc[1:, :]
            df.reset_index(drop=True, inplace=True)
            return df
        except Exception as e:
            print(e)

    def backup(data):
        try:
            df = data.copy()
            return df
        except Exception as e:
            print(e)

    for i in range(1996, 2022, 1):

        data = (
            read_data(i)
            .pipe(backup)
            .pipe(clean_data)
            .pipe(clean_columns)
            .pipe(clean_fecha)
        )

        data.to_csv(relative_path + "\\data_lake\\raw\\{}.csv".format(i), index=False)

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    # transform_data()
    doctest.testmod()
