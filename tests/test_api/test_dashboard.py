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
        """Channel summaries keep channel hash, region, and label metadata."""
        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        assert data["channel_messages"]["A1B2C3@4660"][0]["channel_hash"] == "A1B2C3"
        assert data["channel_messages"]["A1B2C3@4660"][0]["channel_region_flag"] == 4660
        assert data["channel_messages"]["A1B2C3@4660"][0]["channel_name"] == "Ch 1"

    def test_get_stats_keeps_region_variants_separate(
        self, client_no_auth, api_db_session
    ):
        """Dashboard channel groups do not collapse different region variants."""
        fixed_time = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
        api_db_session.add_all(
            [
                Message(
                    message_type="channel",
                    channel_idx=17,
                    channel_hash="11",
                    channel_region_flag=1,
                    text="Public region 1",
                    received_at=fixed_time,
                ),
                Message(
                    message_type="channel",
                    channel_idx=17,
                    channel_hash="11",
                    channel_region_flag=2,
                    text="Public region 2",
                    received_at=fixed_time,
                ),
            ]
        )
        api_db_session.commit()

        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        assert "11@1" in data["channel_messages"]
        assert "11@2" in data["channel_messages"]
        assert data["channel_messages"]["11@1"][0]["channel_name"] == "Public"
        assert data["channel_messages"]["11@2"][0]["channel_name"] == "Public"

    def test_get_stats_hides_encrypted_channel_placeholders(
        self, client_no_auth, api_db_session
    ):
        """Dashboard summaries omit encrypted channel placeholder entries."""
        fixed_time = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
        api_db_session.add_all(
            [
                Message(
                    message_type="channel",
                    channel_idx=202,
                    channel_hash="CAFE01",
                    text="Encrypted channel message",
                    received_at=fixed_time,
                ),
                Message(
                    message_type="channel",
                    channel_idx=202,
                    channel_hash="CAFE01",
                    text="Visible plaintext message",
                    received_at=fixed_time + timedelta(minutes=1),
                ),
            ]
        )
        api_db_session.commit()

        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        channel_messages = data["channel_messages"]["CAFE01"]
        assert len(channel_messages) == 1
        assert channel_messages[0]["text"] == "Visible plaintext message"

    def test_get_stats_uses_bracketed_channel_name_hint(
        self, client_no_auth, api_db_session
    ):
        """Dashboard prefers text prefix channel hints over generic Ch labels."""
        api_db_session.add(
            Message(
                message_type="channel",
                channel_idx=202,
                channel_hash="CAFE01",
                text="[Ops Net] Status update",
                received_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            )
        )
        api_db_session.commit()

        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        channel_message = data["channel_messages"]["CAFE01"][0]
        assert channel_message["channel_name"] == "Ops Net"
        assert channel_message["text"] == "Status update"

    def test_get_stats_prefers_specific_hashtag_hint_over_public_label(
        self, client_no_auth, api_db_session
    ):
        """Dashboard prefers bracketed hashtag labels over generic Public fallback."""
        api_db_session.add(
            Message(
                message_type="channel",
                channel_idx=17,
                channel_hash="11",
                text="[#ops] Status update",
                received_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            )
        )
        api_db_session.commit()

        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        channel_message = data["channel_messages"]["11"][0]
        assert channel_message["channel_name"] == "#ops"
        assert channel_message["text"] == "Status update"

    def test_get_stats_merges_named_channel_groups_with_missing_hash_metadata(
        self, client_no_auth, api_db_session
    ):
        """Dashboard collapses duplicate named hashtag channel groups."""
        api_db_session.add_all(
            [
                Message(
                    message_type="channel",
                    channel_idx=17,
                    channel_hash="11",
                    text="[#ops] Latest status",
                    received_at=datetime(2024, 1, 1, 12, 1, tzinfo=timezone.utc),
                ),
                Message(
                    message_type="channel",
                    channel_idx=17,
                    channel_hash=None,
                    text="[#ops] Earlier status",
                    received_at=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
                ),
            ]
        )
        api_db_session.commit()

        response = client_no_auth.get("/api/v1/dashboard/stats")
        assert response.status_code == 200
        data = response.json()
        channel_messages = data["channel_messages"]

        assert len(channel_messages) == 1
        merged_messages = next(iter(channel_messages.values()))
        assert [msg["text"] for msg in merged_messages] == [
            "Latest status",
            "Earlier status",
        ]
        assert all(msg["channel_name"] == "#ops" for msg in merged_messages)


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
