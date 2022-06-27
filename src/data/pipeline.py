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
import clean_data
from luigi import Task, LocalTarget
import compute_daily_prices
import compute_monthly_prices
import transform_data
import ingest_data
import create_data_lake
from create_data_lake import get_project_root


class CleaningData(Task):
    def output(self):
        self.relative_path = str(get_project_root())
        return LocalTarget(
            self.relative_path + "/data_lake/cleansed/precios-horarios.csv"
        )

    def run(self):
        # try:
        #     create_data_lake.create_data_lake()
        # except Exception as e:
        #     print(e, "create_data_lake_error")
        try:
            ingest_data.ingest_data()
        except Exception as e:
            print(e, "ingest_data_error")
        try:
            transform_data.transform_data()
        except Exception as e:
            print(e, "transform_data_error")
        try:
            df = clean_data.clean_data_pipe()
            file = open(self.output().path, "wb")
            df.to_csv(file, index=False)

        except Exception as e:
            print(e, "clean_data_error")

    # raise NotImplementedError("Implementar esta función")


class MonthlyReport(Task):
    def requires(self):
        return CleaningData()

    def output(self):
        self.relative_path = str(get_project_root())
        return LocalTarget(
            self.relative_path + "/data_lake/business/precios-mensuales.csv"
        )

    def run(self):
        try:
            import pandas as pd

            i = pd.read_csv(self.input().open("r"))
            df = compute_monthly_prices.compute_monthly_prices_pipe(i)
            file = open(self.output().path, "wb")
            df.to_csv(file, index=False)
        except Exception as e:
            print(e, "monthly_report")


class DailyReport(Task):
    def requires(self):
        return CleaningData()

    def output(self):
        self.relative_path = str(get_project_root())
        return LocalTarget(
            self.relative_path + "/data_lake/business/precios-diarios.csv"
        )

    def run(self):
        try:
            import pandas as pd

            i = pd.read_csv(self.input().open("r"))
            df = compute_daily_prices.compute_daily_prices_pipe(i)
            file = open(self.output().path, "wb")
            df.to_csv(file, index=False)
        except Exception as e:
            print(e, "daily_report")


class FinalRun(Task):
    def requires(self):
        return [MonthlyReport(), DailyReport()]


if __name__ == "__main__":
    import doctest

    luigi.run(["FinalRun", "--local-scheduler"])
    doctest.testmod()
