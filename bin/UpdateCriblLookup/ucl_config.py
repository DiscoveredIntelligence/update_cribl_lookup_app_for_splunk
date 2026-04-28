#!/usr/bin/env python3

"""
UCL Configuration Manager - Shared configuration reading for UpdateCriblLookup app
Handles reading leader, worker group, and lookup configurations from Splunk REST endpoints
"""

import logging
import splunk.entity as entity

from solnlib.conf_manager import ConfManager
from typing import Dict, Optional


class UCLConfigManager:
    """
    Manages conf file reading for UpdateCriblLookup
    """
    
    def __init__(self, session_key: str, logger: Optional[logging.Logger] = None, search_id: Optional[str] = None):
        """
        Initialize the configuration manager
        
        Args:
            session_key: Splunk session key for API access
            logger: Optional logger instance
            search_id: Optional search ID for consistent logging
        """
        self.session_key = session_key
        self.logger = logger or logging.getLogger('updatecribllookup')
        self.search_id = search_id
        
    
    def _get_search_id_prefix(self):
        """
        Get search ID prefix for logging consistency.
        Returns search ID prefix if available, empty string otherwise.
        """
        if self.search_id:
            return f"({self.search_id}) "
        return ""


    def get_workergroup_config(self, workergroup_name: str) -> Dict:
        """
        Get worker group configuration and associated leader configuration
        
        Args:
            workergroup_name: Name of the worker group
            
        Returns:
            Dict containing all configuration needed for Cribl sync
            
        Raises:
            Exception: If configuration is not found or invalid
        """
        try:
            msg = f"Getting configuration for worker group: {workergroup_name}"
            self.logger.debug(f"{self._get_search_id_prefix()} {msg}")
            
            self.logger.debug(f"{self._get_search_id_prefix()} Fetching worker group entity from REST API")
            workergroup = entity.getEntity(
                'admin/updatecribllookup_workergroups',
                workergroup_name,
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            if not workergroup:
                raise Exception(f"Worker group '{workergroup_name}' not found in configuration")
            
            self.logger.debug(f"{self._get_search_id_prefix()} Worker group entity retrieved: {dict(workergroup)}")
            
            enabled = workergroup.get('enabled', '1')
            disabled = workergroup.get('disabled', '0')
            
            self.logger.debug(f"{self._get_search_id_prefix()} Worker group enabled: {enabled}, disabled: {disabled}")
            
            # UCC framework uses 'disabled' field instead of 'enabled'
            if enabled == '0' or disabled == '1':
                raise Exception(f"Worker group '{workergroup_name}' is disabled")
            
            associated_leader = workergroup.get('associated_leader_name')
            if not associated_leader:
                raise Exception(f"Worker group '{workergroup_name}' has no associated leader")
            
            self.logger.debug(f"{self._get_search_id_prefix()} Associated leader: {associated_leader}")
            
            leader_config = self.get_leader_config(associated_leader)
            config = {
                'workergroup_name': workergroup_name,
                'associated_leader_name': associated_leader,
                **leader_config
            }
            
            self.logger.debug(f"{self._get_search_id_prefix()} Complete configuration: {config}")
            return config
            
        except Exception as e:
            msg = f"Failed to get configuration for worker group '{workergroup_name}': {str(e)}"
            self.logger.error(f"{self._get_search_id_prefix()} {msg}")
            raise Exception(f"Failed to get configuration for worker group '{workergroup_name}': {str(e)}")
    

    def get_leader_config(self, leader_name: str) -> Dict:
        """
        Get leader configuration
        
        Args:
            leader_name: Name of the leader
            
        Returns:
            Dict containing leader configuration
            
        Raises:
            Exception: If leader configuration is not found or invalid
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Getting leader configuration for: {leader_name}")
            
            leader = entity.getEntity(
                'admin/updatecribllookup_leaders',
                leader_name,
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            if not leader:
                raise Exception(f"Leader '{leader_name}' not found in configuration")
            
            self.logger.debug(f"{self._get_search_id_prefix()} Leader entity retrieved: {dict(leader)}")
            
            # Check if leader is enabled
            enabled = leader.get('enabled', '1')
            disabled = leader.get('disabled', '0')
            
            self.logger.debug(f"{self._get_search_id_prefix()} Leader enabled: {enabled}, disabled: {disabled}")
            
            # Handle both 'enabled' and 'disabled' fields (UCC generates 'disabled')
            if enabled == '0' or disabled == '1':
                raise Exception(f"Leader '{leader_name}' is disabled")
            
            # Extract required fields
            client_id = leader.get('client_id')
            cribl_cloud_url = leader.get('cribl_cloud_url')
            
            self.logger.debug(f"{self._get_search_id_prefix()} Leader client_id: {client_id}")
            self.logger.debug(f"{self._get_search_id_prefix()} Leader cribl_cloud_url: {cribl_cloud_url}")
            
            if not client_id:
                raise Exception(f"Leader '{leader_name}' missing client_id")
            if not cribl_cloud_url:
                raise Exception(f"Leader '{leader_name}' missing cribl_cloud_url")
            
            # Get the encrypted client_secret from passwords.conf
            client_secret = self.get_encrypted_credential(leader_name)
            
            msg = f"Retrieved encrypted client_secret, length: {len(client_secret)}"
            self.logger.debug(f"{self._get_search_id_prefix()} {msg}")
            
            if not client_secret:
                raise Exception(f"Leader '{leader_name}' missing client_secret")
            
            return {
                'client_id': client_id,
                'client_secret': client_secret,
                'cribl_cloud_url': cribl_cloud_url,
                'leader_name': leader_name
            }
            
        except Exception as e:
            msg = f"Failed to get leader configuration for '{leader_name}': {str(e)}"
            self.logger.error(f"{self._get_search_id_prefix()} {msg}")
            raise
    

    def get_lookup_config(self, lookup_name: str) -> Dict:
        """
        Get lookup configuration
        
        Args:
            lookup_name: Name of the lookup
            
        Returns:
            Dict containing lookup configuration
            
        Raises:
            Exception: If lookup configuration is not found or invalid
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Getting lookup configuration for: {lookup_name}")
            
            # Get lookup configuration
            lookup = entity.getEntity(
                'admin/updatecribllookup_lookups',
                lookup_name,
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            if not lookup:
                raise Exception(f"Lookup '{lookup_name}' not found in configuration")
            
            self.logger.debug(f"{self._get_search_id_prefix()} Lookup entity retrieved: {dict(lookup)}")
            
            # Check if lookup is enabled
            enabled = lookup.get('enabled', '1')
            disabled = lookup.get('disabled', '0')
            
            self.logger.debug(f"{self._get_search_id_prefix()} Lookup enabled: {enabled}, disabled: {disabled}")
            
            # Handle both 'enabled' and 'disabled' fields (UCC generates 'disabled')
            if enabled == '0' or disabled == '1':
                raise Exception(f"Lookup '{lookup_name}' is disabled")
            
            return {
                'lookup_name': lookup_name,
                'enabled': enabled == '1' and disabled == '0'
            }
            
        except Exception as e:
            msg = f"Failed to get lookup configuration for '{lookup_name}': {str(e)}"
            self.logger.error(f"{self._get_search_id_prefix()} {msg}")
            raise
    

    def list_workergroups(self) -> Dict:
        """
        List all available worker groups
        
        Returns:
            Dict of worker group configurations
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Listing all worker groups")
            
            workergroups = entity.getEntities(
                'admin/updatecribllookup_workergroups',
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            self.logger.debug(f"{self._get_search_id_prefix()} Found {len(workergroups)} worker groups")
            
            result = {}
            for name, wg in workergroups.items():
                enabled = wg.get('enabled', '1')
                disabled = wg.get('disabled', '0')
                
                result[name] = {
                    'name': name,
                    'enabled': enabled == '1' and disabled == '0',
                    'associated_leader_name': wg.get('associated_leader_name')
                }
            
            return result
            
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()} Failed to list worker groups: {str(e)}")
            raise


    def list_leaders(self) -> Dict:
        """
        List all available leaders
        
        Returns:
            Dict of leader configurations
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Listing all leaders")
            
            leaders = entity.getEntities(
                'admin/updatecribllookup_leaders',
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            self.logger.debug(f"{self._get_search_id_prefix()} Found {len(leaders)} leaders")
            
            result = {}
            for name, leader in leaders.items():
                enabled = leader.get('enabled', '1')
                disabled = leader.get('disabled', '0')
                
                result[name] = {
                    'name': name,
                    'enabled': enabled == '1' and disabled == '0',
                    'cribl_cloud_url': leader.get('cribl_cloud_url'),
                    'client_id': leader.get('client_id')
                }
            
            return result
            
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()} Failed to list leaders: {str(e)}")
            raise
    

    def list_lookups(self) -> Dict:
        """
        List all available lookups
        
        Returns:
            Dict of lookup configurations
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Listing all lookups")
            
            lookups = entity.getEntities(
                'admin/updatecribllookup_lookups',
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            self.logger.debug(f"{self._get_search_id_prefix()} Found {len(lookups)} lookups")
            
            result = {}
            for name, lookup in lookups.items():
                enabled = lookup.get('enabled', '1')
                disabled = lookup.get('disabled', '0')
                
                result[name] = {
                    'name': name,
                    'enabled': enabled == '1' and disabled == '0'
                }
            
            return result
            
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()} Failed to list lookups: {str(e)}")
            raise
    

    def get_log_level(self) -> str:
        """
        Get the configured log level from TA settings
        
        Returns:
            str: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Getting log level from UCL settings")
            
            # Get the logging settings
            settings = entity.getEntity(
                'admin/updatecribllookup_settings',
                'updatecribllookup_logging',
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            if not settings:
                self.logger.warning(f"{self._get_search_id_prefix()} Logging settings not found, defaulting to INFO")
                return 'INFO'
            
            log_level = settings.get('loglevel', 'INFO')
            self.logger.debug(f"{self._get_search_id_prefix()} Retrieved log level: {log_level}")
            
            # Validate log level
            valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            if log_level.upper() not in valid_levels:
                self.logger.warning(f"{self._get_search_id_prefix()} Invalid log level '{log_level}', defaulting to " /
                                    f"INFO")
                return 'INFO'
            
            return log_level.upper()
            
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()} Failed to get log level: {str(e)}")
            return 'INFO'  # Default fallback
    

    def get_http_debug_level(self) -> str:
        """
        Get the configured HTTP debug level from UCL settings
        
        Returns:
            str: HTTP debug level (off, basic, summary, full) - defaults to 'summary'
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Getting HTTP debug level from UCL settings")
            
            # Get the logging settings
            settings = entity.getEntity(
                'admin/updatecribllookup_settings',
                'updatecribllookup_logging',
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            if not settings:
                self.logger.debug(f"{self._get_search_id_prefix()} Logging settings not found, defaulting HTTP debug " /
                                  f"level to 'summary'")
                return 'summary'
            
            http_debug_level = settings.get('http_debug_level', 'summary')
            self.logger.debug(f"{self._get_search_id_prefix()} Retrieved HTTP debug level: {http_debug_level}")
            
            # Validate the setting
            valid_levels = ['off', 'basic', 'summary', 'full']
            if http_debug_level.lower() not in valid_levels:
                self.logger.warning(f"{self._get_search_id_prefix()} Invalid HTTP debug level '{http_debug_level}', " /
                                    f"defaulting to 'summary'")
                return 'summary'
            
            return http_debug_level.lower()
            
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()} Failed to get HTTP debug level: {str(e)}")
            return 'summary'


    def get_skip_mode_detection(self) -> bool:
        """
        Get the skip mode detection setting from TA settings
        
        Returns:
            bool: True to skip mode detection, False to perform it (default: False)
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()}Getting skip mode detection setting from UCL settings")
            
            # Get the logging settings
            settings = entity.getEntity(
                'admin/updatecribllookup_settings',
                'updatecribllookup_logging',
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            if not settings:
                self.logger.debug(f"{self._get_search_id_prefix()} Logging settings not found, defaulting skip mode " /
                                  f"detection to False")
                return False
            
            skip_mode_detection = settings.get('skip_mode_detection', '0')
            self.logger.debug(f"{self._get_search_id_prefix()} Retrieved skip mode detection: {skip_mode_detection}")
            
            # Convert to boolean (1/true/yes = True, anything else = False)
            return skip_mode_detection.lower() in ['1', 'true', 'yes']
            
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()} Failed to get skip mode detection setting: {str(e)}")
            return False
    

    def get_verify_ssl(self) -> bool:
        """
        Get the SSL verification setting from UCL settings
        
        Returns:
            bool: True to verify SSL certificates, False to skip verification
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Getting SSL verification setting from UCL settings")
            
            # Get the network settings
            settings = entity.getEntity(
                'admin/updatecribllookup_settings',
                'updatecribllookup_proxy',
                namespace='UpdateCriblLookup',
                owner='nobody',
                sessionKey=self.session_key
            )
            
            if not settings:
                self.logger.warning(f"{self._get_search_id_prefix()} UCL Network settings not found, defaulting to " /
                                    f"SSL verification enabled")
                return True
            
            verify_ssl = settings.get('verify_ssl', '1')  # Default to '1' (enabled)
            self.logger.debug(f"{self._get_search_id_prefix()} Retrieved verify_ssl setting: {verify_ssl}")
            
            # Splunk stores checkbox values as '1'/'0' strings
            return verify_ssl in ('1', 'true', 'True', True)
            
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()} Failed to get SSL verification setting: {str(e)}")
            return True  # Default to secure
    
    
    def get_encrypted_credential(self, leader_name: str) -> str:
        """
        Get the encrypted client_secret from passwords.conf using solnlib
        
        Args:
            leader_name: Name of the leader stanza
            
        Returns:
            str: The decrypted client_secret
        """
        try:
            self.logger.debug(f"{self._get_search_id_prefix()} Getting encrypted credential for leader: {leader_name}")
            
            # Use ConfManager to access encrypted credentials
            cfm = ConfManager(
                self.session_key, 
                'UpdateCriblLookup', 
                realm="__REST_CREDENTIAL__#UpdateCriblLookup#configs/conf-updatecribllookup_leaders"
            )
            conf = cfm.get_conf('updatecribllookup_leaders')
            leader_details = conf.get(leader_name)
            
            if not leader_details:
                raise Exception(f"Leader '{leader_name}' not found in encrypted storage")
            
            client_secret = leader_details.get('client_secret')
            if not client_secret:
                raise Exception(f"Client secret not found for leader '{leader_name}'")
            
            msg = f"Retrieved encrypted credential, length: {len(client_secret)}"
            self.logger.debug(f"{self._get_search_id_prefix()} {msg}")
            return client_secret
            
        except Exception as e:
            msg = f"Failed to get encrypted credential for leader '{leader_name}': {str(e)}"
            self.logger.error(f"{self._get_search_id_prefix()} {msg}")
            raise

