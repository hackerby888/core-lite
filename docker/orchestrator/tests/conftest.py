import pytest


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Provide a temporary data directory for tests."""
    data_dir = tmp_path / "qubic"
    data_dir.mkdir()
    return data_dir
