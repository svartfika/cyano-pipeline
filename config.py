from typing import Any, Optional
from urllib.parse import urlencode

from pydantic import Field
from pydantic_settings import BaseSettings


class BathingWatersApi(BaseSettings):
    """Swedish bathing waters API client for water quality data and forecasts."""

    host: str = Field(default="https://gw.havochvatten.se", env="API_HOST")

    class Config:
        env_file = ".env"

    @property
    def api_base(self) -> str:
        return f"{self.host}/external-public/bathing-waters/v2"

    def _build_url(self, path: str, params: Optional[dict[str, Any]] = None) -> str:
        """Build URL with optional query parameters."""
        url = f"{self.api_base}{path}"
        if params:
            url += f"?{urlencode(params)}"
        return url

    # Data Endpoints
    def waters(self) -> str:
        """Get all active bathing waters."""
        return self._build_url("/bathing-waters")

    def water(self, bathing_water_id: str) -> str:
        """Get specific bathing water details."""
        return self._build_url(f"/bathing-waters/{bathing_water_id}")

    def results(self, bathing_water_id: str) -> str:
        """Get water quality test results for a bathing water."""
        return self._build_url(f"/bathing-waters/{bathing_water_id}/results")

    def forecasts(self, bathing_water_id: str = None, munic_id: int = None) -> str:
        """Get water quality forecasts by bathing water or municipality."""
        params = {}
        if bathing_water_id:
            params["bathingWaterId"] = bathing_water_id
        if munic_id:
            params["municId"] = munic_id
        return self._build_url("/forecasts", params or None)

    def changelogs(self, filter: str = None) -> str:
        """Get bathing water dataset changes."""
        params = {"filter": filter} if filter else None
        return self._build_url("/bathing-waters/changelogs", params)

    # System Endpoints
    def health(self) -> str:
        """Get API health status."""
        return self._build_url("/operations/health-checks/readiness")

    def metadata(self) -> str:
        """Get API metadata and version info."""
        return self._build_url("/operations/metadata")
