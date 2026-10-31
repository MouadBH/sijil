from sijil_core.settings import CoreSettings


class RegionMismatch(Exception):
    """A resource belongs to a region other than the one this service runs in."""

    code = "region.mismatch"

    def __init__(self, resource_region: str, service_region: str) -> None:
        super().__init__(f"resource in {resource_region!r}, service in {service_region!r}")
        self.resource_region = resource_region
        self.service_region = service_region


def assert_region(resource_region: str, settings: CoreSettings) -> None:
    if resource_region != settings.region:
        raise RegionMismatch(resource_region, settings.region)
