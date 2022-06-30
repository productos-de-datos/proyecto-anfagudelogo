from src.data.compute_monthly_prices import ano_mes


def test_capital_case():
    assert ano_mes("2004-05-12") == "2004-05"
