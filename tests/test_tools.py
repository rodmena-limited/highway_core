import pytest
from unittest.mock import patch, MagicMock
import subprocess
import requests
from highway_core.tools.command import run as command_run
from highway_core.tools.fetch import get as fetch_get, post as fetch_post
from highway_core.tools.memory import set_memory as memory_set_memory


class TestCommandTool:
    """Test the command execution tool."""

    def test_command_success(self):
        """Test running a successful command."""
        with patch("subprocess.run") as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = "Success output"
            mock_result.stderr = ""
            mock_run.return_value = mock_result

            result = command_run("echo hello")

            assert result["status_code"] == 0
            assert result["stdout"] == "Success output"
            assert result["stderr"] == ""

    def test_command_failure(self):
        """Test running a command that fails."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1, cmd="false", output="stdout", stderr="stderr"
            )

            result = command_run("false")

            assert result["status_code"] == 1
            assert result["stderr"] == "stderr"

    def test_command_exception(self):
        """Test handling an exception during command execution."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = Exception("Unexpected error")

            result = command_run("invalid command")

            assert result["status_code"] == -1
            assert result["stderr"] == "Unexpected error"


class TestFetchTool:
    """Test the fetch tool."""

    def test_fetch_get_success(self):
        """Test successful HTTP GET request."""
        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"key": "value"}
            mock_response.headers = {"Content-Type": "application/json"}
            mock_get.return_value = mock_response

            result = fetch_get("https://httpbin.org/json")

            assert result["status"] == 200
            assert result["data"] == {"key": "value"}
            assert result["headers"] == {"Content-Type": "application/json"}

    def test_fetch_get_http_error(self):
        """Test handling of HTTP error."""
        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 404
            mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
                "404 Client Error", response=mock_response
            )
            mock_get.return_value = mock_response

            result = fetch_get("https://httpbin.org/status/404")

            assert result["status"] == 404

    def test_fetch_get_exception(self):
        """Test handling an exception during fetch."""
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.exceptions.RequestException(
                "Connection error"
            )
            mock_get.return_value.status_code = (
                500  # This simulates the exception response attribute
            )

            result = fetch_get("https://invalid-url-that-does-not-exist.com")

            assert result["status"] == 500
            assert result["data"] == "Connection error"

    def test_fetch_post_success(self):
        """Test successful HTTP POST request."""
        with patch("requests.post") as mock_post:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"created": True}
            mock_response.headers = {"Content-Type": "application/json"}
            mock_post.return_value = mock_response

            result = fetch_post("https://httpbin.org/post", data={"test": "value"})

            assert result["status"] == 200
            assert result["data"] == {"created": True}


class TestMemoryTool:
    """Test the memory tool."""

    def test_memory_set(self):
        """Test setting values in memory."""
        from highway_core.engine.state import WorkflowState

        # Create a simple state
        state = WorkflowState({})

        # Set a value using the memory tool
        result = memory_set_memory(state, "key1", "value1")

        # Verify the result structure
        assert result["key"] == "key1"
        assert result["status"] == "ok"

        # Verify that the value is in the state's memory
        assert state.memory["key1"] == "value1"


if __name__ == "__main__":
    pytest.main()
