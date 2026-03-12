"""Deployment file regression tests."""

from pathlib import Path

import yaml


def test_meshcore_services_use_default_repo_image() -> None:
    """Project services should default to this repository's published image."""
    compose_path = (
        Path(__file__).resolve().parents[2]
        / "docker-compose.yml"
    )
    compose_data = yaml.safe_load(compose_path.read_text())

    expected_image = "${IMAGE_NAME:-ghcr.io/dabeani/meshcore-hub}:${IMAGE_VERSION:-latest}"

    project_service_images = {
        name: service["image"]
        for name, service in compose_data["services"].items()
        if service.get("image", "").endswith("${IMAGE_VERSION:-latest}")
    }

    assert project_service_images
    assert set(project_service_images.values()) == {expected_image}
