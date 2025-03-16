import importlib
import os
from typing import List
from services.common_lib.logging_config import logger

class PluginBase:
    def on_bridging_update(self, context: dict):
        pass

class PluginManager:
    def __init__(self):
        self.plugins: List[PluginBase] = []

    def load_plugins(self, plugin_list: list):
        for p in plugin_list:
            if not p['enabled']:
                continue
            module_path = p['plugin_path']
            try:
                module = importlib.import_module(module_path)
                plugin_class = getattr(module, "Plugin", None)
                if plugin_class and issubclass(plugin_class, PluginBase):
                    plugin_instance = plugin_class()
                    self.plugins.append(plugin_instance)
                    logger.info(f"Loaded plugin: {p['plugin_name']}")
            except Exception as e:
                logger.error(f"Error loading plugin {module_path}: {e}")

    def trigger_bridging_update(self, context: dict):
        for plugin in self.plugins:
            plugin.on_bridging_update(context)
