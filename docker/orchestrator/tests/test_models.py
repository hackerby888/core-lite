from app.models import TickInfo, NodeState, NodeHealth, EpochInfo


class TestTickInfo:
    def test_from_json(self):
        data = {
            "epoch": 198,
            "tick": 43101500,
            "initialTick": 43101000,
            "alignedVotes": 500,
            "misalignedVotes": 176,
            "mainAuxStatus": 1,
            "isSavingSnapshot": False,
        }
        tick_info = TickInfo.from_json(data)
        assert tick_info.epoch == 198
        assert tick_info.tick == 43101500
        assert tick_info.initial_tick == 43101000
        assert tick_info.aligned_votes == 500
        assert tick_info.misaligned_votes == 176
        assert tick_info.is_saving_snapshot is False

    def test_from_json_missing_snapshot(self):
        data = {
            "epoch": 198,
            "tick": 43101500,
            "initialTick": 43101000,
            "alignedVotes": 500,
            "misalignedVotes": 176,
            "mainAuxStatus": 1,
        }
        tick_info = TickInfo.from_json(data)
        assert tick_info.is_saving_snapshot is False


class TestNodeState:
    def test_defaults(self):
        state = NodeState()
        assert state.health == NodeHealth.UNKNOWN
        assert state.restart_count == 0
        assert state.consecutive_stuck_polls == 0


class TestEpochInfo:
    def test_creation(self):
        info = EpochInfo(epoch=198, initial_tick=43101000, peers=["1.2.3.4"])
        assert info.epoch == 198
        assert info.initial_tick == 43101000
        assert info.peers == ["1.2.3.4"]
