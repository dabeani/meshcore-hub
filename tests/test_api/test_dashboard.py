"""Tests for dashboard API routes."""

from datetime import datetime, timedelta, timezone

import pytest

from meshcore_hub.common.models import Advertisement, Message, Node


class TestDashboardStats:
    """Tests for GET /dashboard/stats endpoint."""

    def test_get_stats_empty(self, client_no_auth):
        """Test getting stats with empty database."""
        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_nodes"] == 0
        assert data["active_nodes"] == 0
        assert data["total_messages"] == 0
        assert data["messages_today"] == 0
        assert data["total_advertisements"] == 0
        assert data["channel_message_counts"] == {}

    def test_get_stats_with_data(
        self, client_no_auth, sample_node, sample_message, sample_advertisement
    ):
        """Test getting stats with data in database."""
        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["total_nodes"] == 1
        assert data["active_nodes"] == 1  # Node was just created
        assert data["total_messages"] == 1
        assert data["total_advertisements"] == 1

    def test_get_stats_includes_channel_region_metadata(
        self, client_no_auth, sample_message_with_receiver
    ):
        """Channel summaries keep channel hash and region metadata for the UI."""
        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["channel_messages"]["1"][0]["channel_hash"] == "A1B2C3"
        assert data["channel_messages"]["1"][0]["channel_region_flag"] == 4660


class TestDashboardHtmlRemoved:
    """Tests that legacy HTML dashboard endpoint has been removed."""

    def test_dashboard_html_endpoint_removed(self, client_no_auth):
        """Test that GET /dashboard no longer returns HTML (legacy endpoint removed)."""
        response = client_no_auth.get("/api/v1/dashboard")
        assert response.status_code in (404, 405)

    def test_dashboard_html_endpoint_removed_trailing_slash(self, client_no_auth):
        """Test that GET /dashboard/ also returns 404/405."""
        response = client_no_auth.get("/api/v1/dashboard/")
        assert response.status_code in (404, 405)


class TestDashboardAuthenticatedJsonRoutes:
    """Tests that dashboard JSON sub-routes return valid JSON with authentication."""

    def test_stats_returns_json_when_authenticated(self, client_with_auth):
        """Test GET /dashboard/stats returns 200 with valid JSON when authenticated."""
        response = client_with_auth.get(
            "/api/v1/dashboard/stats",
            headers={"Authorization": "Bearer test-read-key"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "total_nodes" in data
        assert "active_nodes" in data
        assert "total_messages" in data
        assert "total_advertisements" in data

    def test_activity_returns_json_when_authenticated(self, client_with_auth):
        """Test GET /dashboard/activity returns 200 with valid JSON when authenticated."""
        response = client_with_auth.get(
            "/api/v1/dashboard/activity",
            headers={"Authorization": "Bearer test-read-key"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "days" in data
        assert "data" in data
        assert isinstance(data["data"], list)

    def test_message_activity_returns_json_when_authenticated(self, client_with_auth):
        """Test GET /dashboard/message-activity returns 200 with valid JSON when authenticated."""
        response = client_with_auth.get(
            "/api/v1/dashboard/message-activity",
            headers={"Authorization": "Bearer test-read-key"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "days" in data
        assert "data" in data
        assert isinstance(data["data"], list)

    def test_node_count_returns_json_when_authenticated(self, client_with_auth):
        """Test GET /dashboard/node-count returns 200 with valid JSON when authenticated."""
        response = client_with_auth.get(
            "/api/v1/dashboard/node-count",
            headers={"Authorization": "Bearer test-read-key"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "days" in data
        assert "data" in data
        assert isinstance(data["data"], list)


class TestDashboardActivity:
    """Tests for GET /dashboard/activity endpoint."""

    @pytest.fixture
    def past_advertisement(self, api_db_session):
        """Create an advertisement from yesterday (since today is excluded)."""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        advert = Advertisement(
            public_key="abc123def456abc123def456abc123de",
            name="TestNode",
            adv_type="REPEATER",
            received_at=yesterday,
        )
        api_db_session.add(advert)
        api_db_session.commit()
        api_db_session.refresh(advert)
        return advert

    def test_get_activity_empty(self, client_no_auth):
        """Test getting activity with empty database."""
        response = client_no_auth.get("/api/v1/dashboard/activity")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 30
        assert len(data["data"]) == 30
        # All counts should be 0
        for point in data["data"]:
            assert point["count"] == 0
            assert "date" in point

    def test_get_activity_custom_days(self, client_no_auth):
        """Test getting activity with custom days parameter."""
        response = client_no_auth.get("/api/v1/dashboard/activity?days=7")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 7
        assert len(data["data"]) == 7

    def test_get_activity_max_days(self, client_no_auth):
        """Test that activity is capped at 90 days."""
        response = client_no_auth.get("/api/v1/dashboard/activity?days=365")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 90
        assert len(data["data"]) == 90

    def test_get_activity_with_data(self, client_no_auth, past_advertisement):
        """Test getting activity with advertisement in database.

        Note: Activity endpoints exclude today's data to avoid showing
        incomplete stats early in the day.
        """
        response = client_no_auth.get("/api/v1/dashboard/activity")
        assert response.status_code == 200
        data = response.json()
        # At least one day should have a count > 0
        total_count = sum(point["count"] for point in data["data"])
        assert total_count >= 1


class TestMessageActivity:
    """Tests for GET /dashboard/message-activity endpoint."""

    @pytest.fixture
    def past_message(self, api_db_session):
        """Create a message from yesterday (since today is excluded)."""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        message = Message(
            message_type="direct",
            pubkey_prefix="abc123",
            text="Hello World",
            received_at=yesterday,
        )
        api_db_session.add(message)
        api_db_session.commit()
        api_db_session.refresh(message)
        return message

    def test_get_message_activity_empty(self, client_no_auth):
        """Test getting message activity with empty database."""
        response = client_no_auth.get("/api/v1/dashboard/message-activity")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 30
        assert len(data["data"]) == 30
        # All counts should be 0
        for point in data["data"]:
            assert point["count"] == 0
            assert "date" in point

    def test_get_message_activity_custom_days(self, client_no_auth):
        """Test getting message activity with custom days parameter."""
        response = client_no_auth.get("/api/v1/dashboard/message-activity?days=7")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 7
        assert len(data["data"]) == 7

    def test_get_message_activity_max_days(self, client_no_auth):
        """Test that message activity is capped at 90 days."""
        response = client_no_auth.get("/api/v1/dashboard/message-activity?days=365")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 90
        assert len(data["data"]) == 90

    def test_get_message_activity_with_data(self, client_no_auth, past_message):
        """Test getting message activity with message in database.

        Note: Activity endpoints exclude today's data to avoid showing
        incomplete stats early in the day.
        """
        response = client_no_auth.get("/api/v1/dashboard/message-activity")
        assert response.status_code == 200
        data = response.json()
        # At least one day should have a count > 0
        total_count = sum(point["count"] for point in data["data"])
        assert total_count >= 1


class TestNodeCountHistory:
    """Tests for GET /dashboard/node-count endpoint."""

    @pytest.fixture
    def past_node(self, api_db_session):
        """Create a node from yesterday (since today is excluded)."""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        node = Node(
            public_key="abc123def456abc123def456abc123de",
            name="Test Node",
            adv_type="REPEATER",
            first_seen=yesterday,
            last_seen=yesterday,
            created_at=yesterday,
        )
        api_db_session.add(node)
        api_db_session.commit()
        api_db_session.refresh(node)
        return node

    def test_get_node_count_empty(self, client_no_auth):
        """Test getting node count with empty database."""
        response = client_no_auth.get("/api/v1/dashboard/node-count")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 30
        assert len(data["data"]) == 30
        # All counts should be 0
        for point in data["data"]:
            assert point["count"] == 0
            assert "date" in point

    def test_get_node_count_custom_days(self, client_no_auth):
        """Test getting node count with custom days parameter."""
        response = client_no_auth.get("/api/v1/dashboard/node-count?days=7")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 7
        assert len(data["data"]) == 7

    def test_get_node_count_max_days(self, client_no_auth):
        """Test that node count is capped at 90 days."""
        response = client_no_auth.get("/api/v1/dashboard/node-count?days=365")
        assert response.status_code == 200
        data = response.json()
        assert data["days"] == 90
        assert len(data["data"]) == 90

    def test_get_node_count_with_data(self, client_no_auth, past_node):
        """Test getting node count with node in database.

        Note: Activity endpoints exclude today's data to avoid showing
        incomplete stats early in the day.
        """
        response = client_no_auth.get("/api/v1/dashboard/node-count")
        assert response.status_code == 200
        data = response.json()
        # At least one day should have a count > 0 (cumulative)
        # The last day should have count >= 1
        assert data["data"][-1]["count"] >= 1
