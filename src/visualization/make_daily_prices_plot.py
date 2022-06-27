def get_project_root():

    from pathlib import Path

    return Path(__file__).parent.parent.parent


def make_daily_prices_plot():
    """Crea un grafico de lines que representa los precios promedios diarios.

    Usando el archivo data_lake/business/precios-diarios.csv, crea un grafico de
    lines que representa los precios promedios diarios.

    El archivo se debe salvar en formato PNG en data_lake/business/reports/figures/daily_prices.png.

    """
    # raise NotImplementedError("Implementar esta función")

    import pandas as pd

    project_path = str(get_project_root())
    path = project_path + "/data_lake/business/precios-diarios.csv"

    df = pd.read_csv(path)
    plot = df.plot(kind="line", x="fecha", y="precio", figsize=[40, 20])
    fig = plot.get_figure()
    fig.savefig(project_path + "/data_lake/business/reports/figures/daily_prices.png")


if __name__ == "__main__":
    import doctest

    make_daily_prices_plot()
    doctest.testmod()
