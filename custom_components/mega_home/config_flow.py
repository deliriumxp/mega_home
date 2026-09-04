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


class MegaHomeConfigFlow(ConfigFlow, domain=DOMAIN):
    """Ask for the manager address and this object's token, then prove both."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            url = user_input[CONF_MANAGER_URL].strip().rstrip("/")
            token = user_input[CONF_TOKEN].strip()
            parsed = urlparse(url)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                errors[CONF_MANAGER_URL] = "invalid_url"
            else:
                client = ManagerClient(
                    async_get_clientsession(
                        self.hass, user_input.get(CONF_VERIFY_SSL, True)
                    ),
                    url,
                    token,
                )
                try:
                    # Proves address, token and reachability in one request, and
                    # it is the same request the coordinator will keep making.
                    await client.async_version()
                except ManagerAuthError:
                    errors[CONF_TOKEN] = "invalid_auth"
                except ManagerError as err:
                    LOGGER.debug("Manager check failed: %s", err)
                    errors["base"] = "cannot_connect"

            if not errors:
                # One entry per object token: re-adding the same object updates
                # the address instead of creating a second home.
                await self.async_set_unique_id(sha256(token.encode()).hexdigest()[:16])
                self._abort_if_unique_id_configured(
                    updates={CONF_MANAGER_URL: url, CONF_TOKEN: token}
                )
                return self.async_create_entry(
                    title=f"Mega Home ({parsed.hostname})",
                    data={
                        CONF_MANAGER_URL: url,
                        CONF_TOKEN: token,
                        CONF_VERIFY_SSL: user_input.get(CONF_VERIFY_SSL, True),
                    },
                )

        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(STEP_USER, user_input),
            errors=errors,
        )
