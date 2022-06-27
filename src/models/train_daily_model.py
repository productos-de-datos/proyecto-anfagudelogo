def get_project_root():

    from pathlib import Path

    return Path(__file__).parent.parent.parent


def train_daily_model():
    """Entrena el modelo de pronóstico de precios diarios.

    Con las features entrene el modelo de proóstico de precios diarios y
    salvelo en models/precios-diarios.pkl


    """

    import pandas as pd
    import statsmodels.api as sm
    import pickle

    project_path = str(get_project_root())

    df = pd.read_csv(project_path + "/data_lake/business/precios-diarios.csv")
    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d")
    df["day_week"] = df["fecha"].dt.day_of_week
    df["number_week"] = df["fecha"].dt.isocalendar().week
    df = df.set_index("fecha")
    df = df.asfreq("D")
    df = df.sort_index()

    df.to_csv(project_path + "/data_lake/business/features/precios_diarios.csv")

    data_train = df[df.index <= "2020-12-31"]
    data_test = df[df.index > "2020-12-31"]

    # print(df.tail(10))

    mod = sm.tsa.statespace.SARIMAX(
        endog=data_train.precio.astype(float),
        exog=data_train[["day_week", "number_week"]].astype(float),
        enforce_stationarity=False,
        enforce_invertibility=False,
    )

    time_series = mod.fit()

    filename = "time_series.pkl"
    pickle.dump(time_series, open(filename, "wb"))

    # result = time_series.forecast(
    #     120,
    #     exog=data_test[["day_week", "number_week"]].astype(float),
    # )

    # print(result)

    # time_series = mod.fit(data_train)

    # forecaster.fit(y=data_train['y'])
    # forecaster

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    train_daily_model()
    doctest.testmod()
