def create_data_lake():
    import os

    """Cree el data lake con sus capas.

    Esta función debe crear la carpeta `data_lake` en la raiz del proyecto. El data lake contiene
    las siguientes subcarpetas:

    ```
    .
    |
    \___ data_lake/
         |___ landing/
         |___ raw/
         |___ cleansed/
         \___ business/
              |___ reports/
              |    |___ figures/
              |___ features/
              |___ forecasts/

    ```


    """
    relative_path = "\\".join(__file__.split("\\")[:-3])
    os.mkdir(os.path.join(relative_path, "data_lake"))
    os.mkdir(os.path.join(relative_path, "data_lake/landing"))
    os.mkdir(os.path.join(relative_path, "data_lake/raw"))
    os.mkdir(os.path.join(relative_path, "data_lake/cleansed"))
    os.mkdir(os.path.join(relative_path, "data_lake/business"))
    os.mkdir(os.path.join(relative_path, "data_lake/business/reports"))
    os.mkdir(os.path.join(relative_path, "data_lake/business/reports/figures"))
    os.mkdir(os.path.join(relative_path, "data_lake/business/features"))
    os.mkdir(os.path.join(relative_path, "data_lake/business/forecasts"))

    # raise NotImplementedError("Implementar esta función")


if __name__ == "__main__":
    import doctest

    create_data_lake()
    doctest.testmod()
