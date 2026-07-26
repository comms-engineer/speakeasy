"""Shared fixtures. Lives here so every federation test file gets the same
two-hub harness without importing fixtures across test modules.
"""

import pytest

from test_federation import Hub


@pytest.fixture
def hubs(tmp_path):
    alpha = Hub(tmp_path / "alpha.db")
    beta = Hub(tmp_path / "beta.db")
    yield alpha, beta
    alpha.close()
    beta.close()
