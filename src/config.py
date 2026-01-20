"""
Configuration Management Module

Handles loading and accessing configuration parameters from YAML files.
Provides centralized configuration management for the entire pipeline.
"""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)
class Config:
    """
    Configuration loader and manager
    
    Loads configuration from YAML file and provides easy access to parameters.
    Supports nested configuration with dot notation access.
    
    Example:
        >>> config = Config("params.yml")
        >>> print(config.train['n_estimators'])
        >>> print(config.get('train', 'n_estimators'))
    """
    def __init__(self, config_path: str = "params.yml"):
         """
        Initialize configuration
        
        Args:
            config_path: Path to YAML configuration file
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file is invalid YAML
        """
         self.config_path = Path(config_path)
         self.params = self._load_config()
         logger.info(f"Configuration loaded from {self.config_path}")

    def _load_config(self) -> Dict[str, Any]:
         """
        Load configuration from YAML file
        
        Returns:
            Dictionary containing configuration parameters
            
        Raises:
            FileNotFoundError: If config file not found
            yaml.YAMLError: If YAML is invalid
        """
         if not self.config_path.exists():
              raise FileNotFoundError(f"Config file {self.config_path} not found.")
         try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            if config is None:
                raise ValueError("Config file is empty or invalid.")
            return config
         
         except yaml.YAMLError as e:
             logger.error(f"Error parsing YAML file: {e}")
             raise
         
    def get(self, section: str, key: Optional[str] = None, default: Any = None) -> Any:
        """
        Get configuration value
        
        Args:
            section: Configuration section name
            key: Optional key within section
            default: Default value if key not found
            
        Returns:
            Configuration value or default
            
        Example:
            >>> config.get('train', 'n_estimators', 100)
        """
        try:
            if key is None:
                return self.params.get(section, default)
            return self.params.get(section, {}).get(key, default)
        except (KeyError, TypeError):
            return default
    
    def set(self, section: str, key: str, value: Any) -> None:
        """
        Set configuration value (runtime only, doesn't save to file)
        
        Args:
            section: Configuration section
            key: Configuration key
            value: Value to set
        """
        if section not in self.params:
            self.params[section] = {}
        self.params[section][key] = value
        logger.debug(f"Config updated: {section}.{key} = {value}")
    
    def save(self, output_path: Optional[str] = None) -> None:
        """
        Save current configuration to YAML file
        
        Args:
            output_path: Path to save config (uses original path if None)
        """
        save_path =  Path(output_path) if output_path else self.config_path

        with open(save_path, 'w') as f:
            yaml.dump(self.params, f, default_flow_style=False, indent=2)
        logger.info(f"Configuration saved to {save_path}")

    @property
    def data(self) -> Dict[str, Any]:
        """Get data configuration section"""
        return self.params.get('data', {})
    
    @property
    def preprocess(self) -> Dict[str, Any]:
        """Get preprocessing configuration section"""
        return self.params.get('preprocess', {})
    
    @property
    def train(self) -> Dict[str, Any]:
        """Get training configuration section"""
        return self.params.get('train', {})
    
    @property
    def evaluate(self) -> Dict[str, Any]:
        """Get evaluation configuration section"""
        return self.params.get('evaluate', {})
    
    @property
    def predict(self) -> Dict[str, Any]:
        """Get prediction configuration section"""
        return self.params.get('predict', {})
    
    def validate(self) -> bool:
        """
        Validate configuration has all required fields
        
        Returns:
            True if valid, raises ValueError if not
            
        Raises:
            ValueError: If required fields are missing
        """
        required_section = ['data', 'preprocess', 'train', 'evaluate']

        for section in required_section:
            if section not in self.params:
                raise ValueError(f"Missing required config section: {section}")
        
        required_data_keys = ['raw_path', 'processed_path', 'test_size', 'random_state']
        for key in required_data_keys:
            if key not in self.data:
                raise ValueError(f"Missing required data config key: {key}")
            
        if 'model_type' not in self.train:
            raise ValueError("Missing required train config key: model_type")
        
        logger.info("Configuration validation passed")
        return True
    
    def __repr__(self) -> str:
        """String representation"""
        return f"Config(path='{self.config_path}', sections={list(self.params.keys())})"
    
    def __str__(self) -> str:
        """Pretty print configuration"""
        return yaml.dump(self.params, default_flow_style=False, indent=2)
    
