def get_project_root():

    from pathlib import Path

    return Path(__file__).parent.parent.parent


def make_forecasts():
    """Construya los pronosticos con el modelo entrenado final.

    Cree el archivo data_lake/business/forecasts/precios-diarios.csv. Este
    archivo contiene tres columnas:

    * La fecha.

    * El precio promedio real de la electricidad.

    * El pronóstico del precio promedio real.


    """

    import pandas as pd
    import statsmodels.api as sm
    import pickle

    project_path = str(get_project_root())

    df = pd.read_csv(project_path + "/data_lake/business/features/precios_diarios.csv")
    df = df.set_index("fecha")
    data_test = df[df.index > "2020-12-31"]

    model = pickle.load(open("precios-diarios.pkl", "rb"))

    result = model.forecast(
        120,
        exog=data_test[["day_week", "number_week"]].astype(float),
    )

    df = data_test.drop(["day_week", "number_week"], axis=1)
    df["precio_forecast"] = result
    df.rename(columns={"precio": "precio_real"}, inplace=True)

    df.to_csv(project_path + "/data_lake/business/forecasts/precios-diarios.csv")

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    make_forecasts()
    doctest.testmod()
