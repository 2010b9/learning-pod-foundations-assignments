from life_expectancy.clean_data.utils.countries import Country


def test_get_countries(all_countries):
    """Test the `get_countries` class method"""
    assert Country.get_countries() == all_countries
