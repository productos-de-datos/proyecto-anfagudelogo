"""
Construya un pipeline de Luigi que:

* Importe los datos xls
* Transforme los datos xls a csv
* Cree la tabla unica de precios horarios.
* Calcule los precios promedios diarios
* Calcule los precios promedios mensuales

En luigi llame las funciones que ya creo.


"""
import luigi
from clean_data import clean_data
from luigi import Task, LocalTarget
import compute_daily_prices
import compute_monthly_prices
import create_data_lake
import transform_data
import ingest_data


class CleaningData(Task):
    def output(self):
        self.relative_path = "\\".join(__file__.split("\\")[:-2])
        return LocalTarget(
            self.relative_path + "\\data_lake\\cleansed\\precios-horarios.csv"
        )

    def run(self):
        try:
            create_data_lake.create_data_lake()
        except Exception as e:
            print(e, "create_data_lake_error")
        try:
            ingest_data.ingest_data()
        except Exception as e:
            print(e, "ingest_data_error")
        try:
            transform_data.transform_data()
        except Exception as e:
            print(e, "transform_data_error")
        try:
            df = clean_data()
            file = open(self.output().path, "wb")
            df.to_csv(file, index=False)

        except Exception as e:
            print(e, "clean_data_error")

    # raise NotImplementedError("Implementar esta función")


class MonthlyReport:
    def requires(self):
        return CleaningData()

    def output(self):
        self.relative_path = "\\".join(__file__.split("\\")[:-2])
        return LocalTarget(
            self.relative_path + "\\data_lake\\business\\precios-diarios.csv"
        )

    def run(self):
        try:
            import pandas as pd

            i = pd.read_csv(self.input().open("r"))
            df = compute_monthly_prices.compute_monthly_prices(i)
            file = open(self.output().path, "wb")
            df.to_csv(file, index=False)
        except Exception as e:
            print(e, "monthly_report")


if __name__ == "__main__":
    import doctest

    luigi.run(["CleaningData", "--local-scheduler"])
    doctest.testmod()
