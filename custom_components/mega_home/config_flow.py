"""Config flow: where the manager is, and which object this is."""

from __future__ import annotations

from hashlib import sha256
from typing import Any
from urllib.parse import urlparse

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import ManagerAuthError, ManagerClient, ManagerError
from .const import (
    CONF_MANAGER_URL,
    CONF_TOKEN,
    CONF_VERIFY_SSL,
    DOMAIN,
    LOGGER,
)

STEP_USER = vol.Schema(
    {
        vol.Required(CONF_MANAGER_URL): str,
        vol.Required(CONF_TOKEN): str,
        vol.Optional(CONF_VERIFY_SSL, default=True): bool,
    }
)


def check_url(url: str) -> str | None:
    """Return the error key for an unusable manager address, else None."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return "invalid_url"
    return None


class MegaHomeConfigFlow(ConfigFlow, domain=DOMAIN):
    """Ask for the manager address and this object's token, then prove both."""

    VERSION = 1

    async def _async_validate(self, user_input: dict[str, Any]) -> dict[str, str]:
        """Prove the address and the token; returns per-field errors."""
        url = user_input[CONF_MANAGER_URL]
        problem = check_url(url)
        if problem:
            return {CONF_MANAGER_URL: problem}
        client = ManagerClient(
            async_get_clientsession(self.hass, user_input.get(CONF_VERIFY_SSL, True)),
            url,
            user_input[CONF_TOKEN],
        )
        try:
            # Proves address, token and reachability in one request, and it is
            # the same request the coordinator will keep making.
            await client.async_version()
        except ManagerAuthError:
            return {CONF_TOKEN: "invalid_auth"}
        except ManagerError as err:
            LOGGER.debug("Manager check failed: %s", err)
            return {"base": "cannot_connect"}
        return {}

    @staticmethod
    def _clean(user_input: dict[str, Any]) -> dict[str, Any]:
        return {
            CONF_MANAGER_URL: user_input[CONF_MANAGER_URL].strip().rstrip("/"),
            CONF_TOKEN: user_input[CONF_TOKEN].strip(),
            CONF_VERIFY_SSL: user_input.get(CONF_VERIFY_SSL, True),
        }

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            data = self._clean(user_input)
            errors = await self._async_validate(data)
            if not errors:
                # One entry per object token: re-adding the same object updates
                # the address instead of creating a second home. ⚠ verify_ssl
                # едет в updates вместе с адресом: без него повторное добавление
                # меняло адрес, но оставляло прежнюю проверку сертификата —
                # переезд с прокси на порт 8055 с самоподписанным TLS молча не
                # работал.
                await self.async_set_unique_id(
                    sha256(data[CONF_TOKEN].encode()).hexdigest()[:16]
                )
                self._abort_if_unique_id_configured(updates=data)
                return self.async_create_entry(
                    title=f"Mega Home ({urlparse(data[CONF_MANAGER_URL]).hostname})",
                    data=data,
                )

        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(STEP_USER, user_input),
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Change the manager address without losing the entry.

        ⚠ Нужен именно он: адрес менеджера меняется в жизни объекта не раз —
        переезд за обратный прокси и обратно, смена домена, переход на порт 8055
        напрямую. Без этого шага единственным способом был «удалить и добавить
        заново», то есть потеря записи и объяснение по телефону.
        """
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            data = self._clean(user_input)
            errors = await self._async_validate(data)
            if not errors:
                return self.async_update_reload_and_abort(entry, data_updates=data)

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=self.add_suggested_values_to_schema(
                STEP_USER, user_input or dict(entry.data)
            ),
            errors=errors,
        )
