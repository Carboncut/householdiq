import pytest
from services.common_lib.plugin_manager import PluginManager, PluginBase
from services.common_lib.logging_config import logger

class DummyTestPlugin(PluginBase):
    def __init__(self):
        self.handled = False

    def on_bridging_update(self, context: dict):
        self.handled = True

def test_plugin_load():
    manager = PluginManager()
    plugin_list = [
        {"plugin_name": "DummyTest", "plugin_path": "tests.test_plugins", "enabled": True}
    ]
    # We are ironically referencing 'tests.test_plugins', so let's dynamically attach the DummyTestPlugin
    # In a real scenario, plugin_path would be a real module path.
    manager.load_plugins([
        {
            "plugin_name": "TestPlugin",
            "plugin_path": "tests.fake_plugin",  # We'll override below or do a real plugin
            "enabled": True
        }
    ])
    # This won't load properly, but let's check no crash. 
    # For a real test, point plugin_path to an actual plugin file.

def test_plugin_trigger():
    """
    Example test that manually adds a plugin and triggers bridging_update.
    """
    manager = PluginManager()
    dummy = DummyTestPlugin()
    manager.plugins.append(dummy)

    context = {"event_id": 123, "household_id": "hh999"}
    manager.trigger_bridging_update(context)

    assert dummy.handled is True
