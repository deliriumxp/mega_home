"""Эталон RFC 7616 §3.9.1: MD5 и SHA-256 на тех же значениях, что в тексте RFC."""

from __future__ import annotations

from mega_home.core import digest

USERNAME = "Mufasa"
REALM = "http-auth@example.org"
PASSWORD = "Circle of Life"
NONCE = "7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v"
OPAQUE = "FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS"
CNONCE = "f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ"
URI = "/dir/index.html"


def _challenge(algorithm: str) -> dict[str, str]:
    return {
        "realm": REALM,
        "qop": "auth, auth-int",
        "algorithm": algorithm,
        "nonce": NONCE,
        "opaque": OPAQUE,
    }


def test_разбор_вызова_www_authenticate() -> None:
    header = (
        'Digest realm="http-auth@example.org", qop="auth, auth-int", '
        'algorithm=MD5, nonce="7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v", '
        'opaque="FQhe/qaU925kfnzjCev0ciny7QMkPqMAFRtzCUYo5tdS"'
    )
    parsed = digest.parse_www_auth(header)
    assert parsed["realm"] == REALM
    assert parsed["nonce"] == NONCE
    assert parsed["opaque"] == OPAQUE
    assert parsed["algorithm"] == "MD5"
    assert parsed["qop"] == "auth, auth-int"


def test_md5_ответ_совпадает_с_рфс() -> None:
    header = digest.authorization(
        "GET", URI, USERNAME, PASSWORD, _challenge("MD5"), cnonce=CNONCE, nc=1
    )
    assert 'response="8ca523f5e9506fed4657c9700eebdbec"' in header
    assert f'username="{USERNAME}"' in header
    assert f'nonce="{NONCE}"' in header
    assert f'uri="{URI}"' in header
    assert "qop=auth" in header
    assert "nc=00000001" in header
    assert f'cnonce="{CNONCE}"' in header
    assert f'opaque="{OPAQUE}"' in header


def test_sha256_ответ_совпадает_с_рфс() -> None:
    header = digest.authorization(
        "GET", URI, USERNAME, PASSWORD, _challenge("SHA-256"), cnonce=CNONCE, nc=1
    )
    assert (
        'response="753927fa0e85d155564e2e272a28d1802ca10daf4496794697cf8db5856cb6c1"'
        in header
    )
    assert "algorithm=SHA-256" in header


def test_пароль_и_учётка_не_в_исключении() -> None:
    """Ошибку разбора текстом пароль и логин не несут — только формат."""
    try:
        digest.parse_www_auth("Basic realm=x")
    except ValueError as err:
        assert PASSWORD not in str(err)
        assert USERNAME not in str(err)
