#!/usr/bin/env python3
"""
Shared processing logic for Cribl lookup updates.
"""

import logging

from UpdateCriblLookup.cribl_sync_engine import CriblSyncEngine
from splunk.clilib.bundle_paths import make_splunkhome_path
from splunk.clilib.cli_common import getConfKeyValue
from UpdateCriblLookup.ucl_config import UCLConfigManager
from UpdateCriblLookup.updatecribllookup_exceptions import (
    WorkergroupNotProvidedException, 
    LookupNotProvidedException,
    WorkergroupNotFoundException,
    LookupNotFoundException
)


class CriblLookupProcessor:
    """Handles Cribl lookup updates for both streaming commands and alert actions."""

    
    def __init__(self, session_key, logger_name='updatecribllookup', search_id=None):
        self.session_key = session_key
        self.search_id = search_id  # Store search ID context for consistent logging
        self.logger = self.setup_logging(logger_name)
        self.config_manager = UCLConfigManager(session_key, self.logger, search_id)
        
        # Initialize configurations as None - will be loaded on first access
        self.workergroup_configs = None
        self.lookup_configs = None
        
        # Setup retry configuration
        self.retry_config = {
            'max_retries': 3,
            'retry_delay_base': 2,
            'deploy_max_wait_time': 1200,  # 10 minutes for deployment timeout
            'deploy_retry_interval': 30   # 30 second base retry interval for deployments
        }
        
        # Initialize settings as None - will be loaded on first access
        self.http_debug_level = None
        self.skip_mode_detection = None
        
        # If search_id provided, load configurations with proper logging context
        if search_id:
            self._load_configurations()


    def set_search_context(self, search_id):
        """
        Set search ID context and load configurations if not already loaded.
        This allows for proper SID-prefixed logging during configuration loading.
        """
        self.search_id = search_id
        if self.workergroup_configs is None or self.lookup_configs is None:
            self._load_configurations()


    def _load_configurations(self):
        """
        Load configurations with proper search ID context for logging.
        """
        search_id_prefix = self._get_search_id_prefix(self.search_id)
        
        # Load configurations with SID-prefixed logging
        self.workergroup_configs = self.config_manager.list_workergroups()
        self.lookup_configs = self.config_manager.list_lookups()
        
        # Get HTTP debug level setting
        self.http_debug_level = self.config_manager.get_http_debug_level()
        self.logger.debug(f"{search_id_prefix}HTTP debug level: {self.http_debug_level}")
        
        # Get skip mode detection setting
        self.skip_mode_detection = self.config_manager.get_skip_mode_detection()
        self.logger.debug(f"{search_id_prefix}Skip mode detection: {self.skip_mode_detection}")
        
        config_count = f"{len(self.workergroup_configs)} workergroups, {len(self.lookup_configs)} lookups"
        self.logger.debug(f"{search_id_prefix}CriblLookupProcessor initialized with {config_count}")


    def _ensure_configurations_loaded(self):
        """
        Ensure configurations are loaded, loading them if necessary.
        """
        if self.workergroup_configs is None or self.lookup_configs is None:
            self._load_configurations()


    def get_http_debug_level(self):
        """Get HTTP debug level, ensuring configurations are loaded."""
        self._ensure_configurations_loaded()
        return self.http_debug_level


    def get_skip_mode_detection(self):
        """Get skip mode detection setting, ensuring configurations are loaded."""
        self._ensure_configurations_loaded()
        return self.skip_mode_detection


    def _get_search_id_prefix(self, search_id=None):
        """
        Get search ID prefix for logging consistency.
        Returns search ID prefix if provided, empty string otherwise.
        """
        if search_id:
            return f"({search_id}) "
        return ""


    def setup_logging(self, logger_name):
        """Configure logging with app specific settings."""
        try:
            if self.session_key:
                temp_config_manager = UCLConfigManager(self.session_key)
                log_level = temp_config_manager.get_log_level()
            else:
                # Fallback to config file if no session key
                log_level = getConfKeyValue('updatecribllookup_settings', 'logging', 'loglevel') or 'INFO'
        except Exception:
            # If config doesn't exist or has issues, default to INFO
            log_level = 'INFO'
        
        logger = logging.getLogger(logger_name)
        logger.setLevel(getattr(logging, log_level.upper()))
        
        logger.propagate = False  # Avoid duplicate logs
        
        if not logger.handlers:
            log_file = make_splunkhome_path(['var', 'log', 'splunk', 'updatecribllookup.log'])
            file_handler = logging.FileHandler(log_file)
            
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            
            logger.addHandler(file_handler)
        
        return logger


    @staticmethod
    def transform_workergroup_name(splunk_workergroup_name):
        """
        Transform Splunk workergroup name to actual Cribl workergroup name.
        
        Since 'default' is a reserved word in Splunk, we use 'cribl_default' in Splunk
        configuration but need to map it back to 'default' for Cribl API calls.
        
        Args:
            splunk_workergroup_name: The workergroup name from Splunk configuration
            
        Returns:
            str: The actual Cribl workergroup name to use in API calls
        """
        # 'default' is reserved in Splunk, so we use 'cribl_default' instead
        if splunk_workergroup_name == 'cribl_default':
            return 'default'
        
        return splunk_workergroup_name


    def check_workergroup_exists(self, workergroup_name):
        """Check if a workergroup configuration exists."""
        try:
            workergroups = self.config_manager.list_workergroups()
            return workergroup_name in workergroups
        except Exception as e:
            self.logger.error(f"Error checking if workergroup exists: {e}")
            # If we can't check, assume it doesn't exist to be safe
            return False


    def check_lookup_exists(self, lookup_name):
        """Check if a lookup configuration exists."""
        try:
            lookups = self.config_manager.list_lookups()
            return lookup_name in lookups
        except Exception as e:
            self.logger.error(f"Error checking if lookup exists: {e}")
            return False  # Fail safe if we can't verify


    @staticmethod
    def convert_records_to_lookup_data(records):
        """Convert record list to lookup data format."""
        lookup_data = []
        for record in records:
            row = {}
            for key, value in record.items():
                row[key] = str(value) if value is not None else ''
            lookup_data.append(row)
        return lookup_data


    def get_verify_ssl(self):
        """Get SSL verification setting from configuration."""
        try:
            return self.config_manager.get_verify_ssl()
        except Exception as e:
            self.logger.warning(f"Could not get SSL verification setting: {e}")
            return True  # Default to secure setting


    def validate_parameters(self, workergroup, lookup_name):
        """
        Validate required parameters and configurations.
        
        Args:
            workergroup: Workergroup name to validate
            lookup_name: Lookup name to validate
            
        Raises:
            WorkergroupNotProvidedException: If workergroup is not provided
            LookupNotProvidedException: If lookup is not provided
            WorkergroupNotFoundException: If workergroup config doesn't exist
            LookupNotFoundException: If lookup config doesn't exist
        """
        if not workergroup:
            raise WorkergroupNotProvidedException("Worker group must be provided")
        
        if not lookup_name:
            raise LookupNotProvidedException("Lookup name must be provided")

        # Ensure configurations are loaded before validation
        self._ensure_configurations_loaded()

        if workergroup not in self.workergroup_configs:
            raise WorkergroupNotFoundException(f"Workergroup '{workergroup}' not found in configuration")

        if lookup_name not in self.lookup_configs:
            raise LookupNotFoundException(f"Lookup '{lookup_name}' not found in configuration")


    def process_lookup_update(self, workergroup, lookup_name, 
                              records, lookup_mode="auto", commit_message=None, 
                              search_name=None, search_id=None, progress_callback=None):
        """
        Process lookup update for a specific worker group with enhanced error handling.
        
        Args:
            workergroup: Worker group name
            lookup_name: Lookup file name
            records: Search results/records to sync
            lookup_mode: Lookup mode ("auto", "memory", "disk")
            commit_message: Optional custom commit message
            search_name: Optional saved search name
            search_id: Optional search ID (SID)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            search_id_prefix = self._get_search_id_prefix(search_id)
            self.logger.info(f"{search_id_prefix}Processing lookup update for worker group: {workergroup}")
            self.logger.debug(f"{search_id_prefix}Lookup: {lookup_name}, Mode: {lookup_mode}, Records: {len(records)}")
            if commit_message:
                self.logger.debug(f"{search_id_prefix}Custom commit message provided: {commit_message}")
            if search_name:
                self.logger.debug(f"{search_id_prefix}Search name: {search_name}")
            elif search_id:
                self.logger.debug(f"{search_id_prefix}Search ID: {search_id}")
            
            self.validate_parameters(workergroup, lookup_name)
            
            workergroup_config = self.config_manager.get_workergroup_config(workergroup)
            self.logger.debug(f"{search_id_prefix}Worker group config loaded for: {workergroup}")
            
            lookup_config = self.config_manager.get_lookup_config(lookup_name)
            if lookup_mode == "auto" and lookup_config:
                # If auto mode and we have config, use the configured mode
                lookup_mode = lookup_config.get('lookup_mode', 'auto')
                self.logger.debug(f"{search_id_prefix}Auto mode resolved to: {lookup_mode}")
            
            # Convert records to lookup data format
            lookup_data = self.convert_records_to_lookup_data(records)
            self.logger.debug(f"{search_id_prefix}Converted {len(records)} records to lookup data format")
            
            # Transform workergroup name for Cribl API (cribl_default -> default)
            cribl_workergroup_name = self.transform_workergroup_name(workergroup)
            if cribl_workergroup_name != workergroup:
                msg = f"{search_id_prefix}Transformed: {workergroup} -> {cribl_workergroup_name}"
                self.logger.debug(msg)
            
            verify_ssl = self.get_verify_ssl()
            self.logger.debug(f"{search_id_prefix}SSL verification: {verify_ssl}")
            
            cribl_engine = CriblSyncEngine(
                cribl_url=workergroup_config['cribl_cloud_url'],
                client_id=workergroup_config['client_id'],
                client_secret=workergroup_config['client_secret'],
                session_key=self.session_key,
                logger=self.logger,
                max_retries=self.retry_config.get('max_retries', 3),
                retry_delay_base=self.retry_config.get('retry_delay_base', 2),
                verify_ssl=verify_ssl,
                deploy_max_wait_time=self.retry_config.get('deploy_max_wait_time', 1200),
                deploy_retry_interval=self.retry_config.get('deploy_retry_interval', 30),
                http_debug_level=self.get_http_debug_level(),
                skip_mode_detection=self.get_skip_mode_detection()
            )
            
            url = workergroup_config['cribl_cloud_url']
            self.logger.debug(f"{search_id_prefix}CriblSyncEngine initialized with URL: {url}")
            
            # Perform the sync operation
            sync_msg = (
                f"{search_id_prefix}Starting Cribl sync for {lookup_name} to workergroup {workergroup} "
                f"(Cribl: {cribl_workergroup_name})"
            )
            self.logger.info(sync_msg)
            
            success = cribl_engine.sync_lookup(
                leader_config=workergroup_config,
                workergroup=cribl_workergroup_name,
                lookup=lookup_name,
                lookup_data=lookup_data,
                lookup_mode=lookup_mode,
                commit_message=commit_message,
                search_name=search_name,
                search_id=search_id,
                progress_callback=progress_callback
            )
            
            if success:
                success_msg = (
                    f"{search_id_prefix}Successfully synced {len(records)} records to Cribl lookup {lookup_name} "
                    f"in worker group {workergroup}"
                )
                self.logger.info(success_msg)
                return True
            else:
                self.logger.error(f"{search_id_prefix}Cribl sync operation failed for worker group {workergroup}")
                return False
            
        except Exception as e:
            self.logger.error(f"{search_id_prefix}Error processing worker group {workergroup}: {str(e)}")
            self.logger.debug(f"{search_id_prefix}Exception details: {type(e).__name__}: {str(e)}")
            raise


    def process_lookup_update_with_progress(self, workergroup, lookup_name, records, lookup_mode="auto", 
                                            commit_message=None, search_name=None, search_id=None):
        """
        Generator-based lookup update that yields progress messages for EventingCommand.
        
        Args:
            workergroup: Worker group name
            lookup_name: Lookup file name
            records: Search results/records to sync
            lookup_mode: Lookup mode ("auto", "memory", "disk")
            commit_message: Optional custom commit message
            search_name: Optional saved search name
            search_id: Optional search ID (SID)
            
        Yields:
            dict: Progress messages with 'type' ('progress', 'success', 'error') and 'message'
        """
        try:
            search_id_prefix = self._get_search_id_prefix(search_id)
            msg = f'{search_id_prefix}Processing lookup update for worker group: {workergroup}'
            yield {'type': 'progress', 'message': msg}
            self.logger.info(f"{search_id_prefix}Processing lookup update for worker group: {workergroup}")
            self.logger.debug(f"{search_id_prefix}Lookup: {lookup_name}, Mode: {lookup_mode}, Records: {len(records)}")
            if commit_message:
                self.logger.debug(f"{search_id_prefix}Custom commit message provided: {commit_message}")
            if search_name:
                self.logger.debug(f"{search_id_prefix}Search name: {search_name}")
            elif search_id:
                self.logger.debug(f"{search_id_prefix}Search ID: {search_id}")
            
            self.validate_parameters(workergroup, lookup_name)
            msg = f'{search_id_prefix}Configuration validation passed for all parameters'
            yield {'type': 'progress', 'message': msg}
            
            workergroup_config = self.config_manager.get_workergroup_config(workergroup)
            self.logger.debug(f"{search_id_prefix}Worker group config loaded for: {workergroup}")
            
            lookup_config = self.config_manager.get_lookup_config(lookup_name)
            if lookup_mode == "auto" and lookup_config:
                # If auto mode and we have config, use the configured mode
                lookup_mode = lookup_config.get('lookup_mode', 'auto')
                self.logger.debug(f"{search_id_prefix}Auto mode resolved to: {lookup_mode}")
            
            # Convert records to lookup data format
            lookup_data = self.convert_records_to_lookup_data(records)
            self.logger.debug(f"{search_id_prefix}Converted {len(records)} records to lookup data format")
            msg = f'{search_id_prefix}Converted {len(records)} records to lookup data format'
            yield {'type': 'progress', 'message': msg}
            
            # Transform workergroup name for Cribl API (cribl_default -> default)
            cribl_workergroup_name = self.transform_workergroup_name(workergroup)
            if cribl_workergroup_name != workergroup:
                msg = f"{search_id_prefix}Transformed: {workergroup} -> {cribl_workergroup_name}"
                self.logger.debug(msg)
            
            verify_ssl = self.get_verify_ssl()
            skip_mode_detection = self.config_manager.get_skip_mode_detection()
            ssl_msg = f"{search_id_prefix}SSL verification: {verify_ssl}, Skip mode detection: {skip_mode_detection}"
            self.logger.debug(ssl_msg)
            
            cribl_engine = CriblSyncEngine(
                cribl_url=workergroup_config['cribl_cloud_url'],
                client_id=workergroup_config['client_id'],
                client_secret=workergroup_config['client_secret'],
                session_key=self.session_key,
                logger=self.logger,
                max_retries=self.retry_config.get('max_retries', 3),
                retry_delay_base=self.retry_config.get('retry_delay_base', 2),
                verify_ssl=verify_ssl,
                deploy_max_wait_time=self.retry_config.get('deploy_max_wait_time', 1200),
                deploy_retry_interval=self.retry_config.get('deploy_retry_interval', 30),
                http_debug_level=self.get_http_debug_level(),
                skip_mode_detection=self.get_skip_mode_detection()
            )
            
            url = workergroup_config["cribl_cloud_url"]
            msg = f'{search_id_prefix}CriblSyncEngine initialized with URL: {url}'
            self.logger.debug(msg)
            yield {'type': 'progress', 'message': msg}
            
            # Perform the sync operation with progress yielding
            sync_msg = (
                f"{search_id_prefix}Starting Cribl sync for {lookup_name} to workergroup {workergroup} "
                f"(Cribl: {cribl_workergroup_name})"
            )
            self.logger.info(sync_msg)
            yield {'type': 'progress', 'message': sync_msg}
            
            # Use generator-based sync with progress yielding
            for progress_result in cribl_engine.sync_lookup_with_progress(
                leader_config=workergroup_config,
                workergroup=cribl_workergroup_name,
                lookup=lookup_name,
                lookup_data=lookup_data,
                lookup_mode=lookup_mode,
                commit_message=commit_message,
                search_name=search_name,
                search_id=search_id
            ):
                if progress_result.get('type') == 'progress':
                    yield progress_result
                elif progress_result.get('type') == 'success':
                    success_msg = (
                        f"{search_id_prefix}Successfully synced {len(records)} records to Cribl lookup {lookup_name} "
                        f"in worker group {workergroup}"
                    )
                    self.logger.info(success_msg)
                    yield {'type': 'success', 'message': success_msg}
                    return
                elif progress_result.get('type') == 'error':
                    err_msg = progress_result.get('message')
                    self.logger.error(f"{search_id_prefix}Cribl sync operation failed for {workergroup}: {err_msg}")
                    yield progress_result
                    return
            
        except Exception as e:
            error_msg = f"{search_id_prefix}Error processing worker group {workergroup}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(f"{search_id_prefix}Exception details: {type(e).__name__}: {str(e)}")
            yield {'type': 'error', 'message': error_msg}


    def process_multiple_workergroups(self, workergroups, lookup_name, records, lookup_mode="auto",
                                      commit_message=None, search_name=None, search_id=None):
        """
        Process lookup updates for multiple worker groups.
        
        Args:
            workergroups: List of worker group names or comma-separated string
            lookup_name: Lookup file name
            records: Search results/records to sync
            lookup_mode: Lookup mode ("auto", "memory", "disk")
            commit_message: Optional custom commit message
            search_name: Optional saved search name
            search_id: Optional search ID (SID)
            
        Returns:
            bool: True if all updates successful, False otherwise
        """
        try:
            # Handle both list and comma-separated string formats
            if isinstance(workergroups, str):
                worker_groups = [wg.strip() for wg in workergroups.split(',') if wg.strip()]
            else:
                worker_groups = workergroups
            
            self.logger.info(f"Processing {len(worker_groups)} worker groups: {worker_groups}")
            if commit_message:
                self.logger.debug(f"Using custom commit message: {commit_message}")
            
            all_successful = True
            
            for workergroup in worker_groups:
                try:
                    success = self.process_lookup_update(workergroup, lookup_name, records, lookup_mode, 
                                                         commit_message, search_name, search_id)
                    if not success:
                        all_successful = False
                except Exception as e:
                    self.logger.error(f"Failed to process worker group {workergroup}: {e}")
                    all_successful = False
            
            return all_successful
            
        except Exception as e:
            self.logger.error(f"Error processing multiple worker groups: {e}")
            raise

