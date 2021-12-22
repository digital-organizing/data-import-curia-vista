import pytest

from odata2sql.sql import to_snake_case, to_pg_name


def test_to_snake_case():
    assert to_snake_case('CamelCase') == 'camel_case'
    assert to_snake_case('getHTTPResponseCode') == 'get_http_response_code'


def test_to_pg_name():
    assert to_pg_name('ZONE') == '"zone"', "Keywords must be quoted"
    assert to_pg_name('zone') == '"zone"', "Keywords must be quoted"
    assert to_pg_name('not_zone') == 'not_zone', "Non-Keywords must not be quoted"

    with pytest.raises(ValueError):
        to_pg_name("Suspici;ous")
