#!/usr/bin/env python3

"""
Splunk Modular Alert Action: Update Cribl Lookup

This script is the main entry point for the "Update Cribl Lookup" modular alert action.
It has been refactored to correctly use the installed version of the ModularAlertBase
framework from splunktaucclib and to consolidate logging into a single file.

Created by: Discovered Intelligence
            support@discoveredintelligence.ca
"""

import import_declare_test
import os
import sys
import traceback

from UpdateCriblLookup.cribl_lookup_processor import CriblLookupProcessor
from splunktaucclib.alert_actions_base import ModularAlertBase


# Define the alert action class, inheriting from the standard base class
class UpdateCriblLookupAlert(ModularAlertBase):

    def __init__(self, ta_name, alert_name):
        """
        Initialize the modular alert. The base class handles all the initial setup.
        """
        super(UpdateCriblLookupAlert, self).__init__(ta_name, alert_name)
        
        # Initialize search ID context
        self.search_id = None
        self.search_name = None
        self.processor = None  # Will be initialized in process_event with search context


    def _get_search_id_prefix(self):
        """
        Get search ID for logging.

        Returns     search ID prefix if available, empty string otherwise.
        """
        if self.search_id:
            return f"({self.search_id}) "
        return ""


    def process_event(self):
        """
        Main execution logic for the alert action.
        This method is called by the base class's run() method.
        """
        
        try:
            # Extract search context
            search_name = None
            search_id = None
            
            # Try to get search context from search results info
            try:
                if hasattr(self, 'search_results_info') and self.search_results_info:
                    search_info = self.search_results_info
                    search_name = (getattr(search_info, 'label', None) or 
                                 getattr(search_info, 'savedsearch_name', None) or 
                                 getattr(search_info, 'saved_search', None))
                
                # Get SID from session info if available  
                if hasattr(self, 'sid') and self.sid:
                    search_id = self.sid
            except Exception as e:
                # Can't use search ID prefix yet since we haven't set it
                self.logger.debug(f"Could not extract search context: {e}")
            
            # Store search context for logging (BEFORE any other processing)
            self.search_id = search_id
            self.search_name = search_name
            
            # Initialize processor NOW with search context for proper SID-prefixed logging from the start
            self.processor = CriblLookupProcessor(self.session_key, 'updatecribllookup', search_id)
            
            # Replace the base class's logger with the one from the processor class,
            # this way all logs for this will go to the same updatecribllookup.log
            self.logger = self.processor.logger
            
            msg = "Starting Update Cribl Lookup alert action logic in process_event"
            self.logger.info(f"{self._get_search_id_prefix()} {msg}")
            
            # Get configuration items and session key
            cribl_worker_group = self.get_param('cribl_worker_group')
            lookup_file_name = self.get_param('lookup_file_name')
            commit_message = self.get_param('commit_message')  # Optional custom commit message
            
            session_key = self.session_key
            if not session_key:
                self.log_error(f"{self._get_search_id_prefix()} No session key found. This is required for API calls.")
                raise Exception("No session key available for API calls")

            # Validate parameters before processing
            self.processor.validate_parameters(cribl_worker_group, lookup_file_name)

            # The get_events() method handles reading the gzipped results file
            results = list(self.get_events())
            
            self.logger.info(f"{self._get_search_id_prefix()} Processing alert with worker group: {cribl_worker_group}")
            self.logger.info(f"{self._get_search_id_prefix()} Lookup file name: {lookup_file_name}")
            if commit_message:
                self.logger.info(f"{self._get_search_id_prefix()} Using custom commit message: {commit_message}")
            else:
                self.logger.info(f"{self._get_search_id_prefix()} No custom commit message provided, will use default")
            if search_name:
                self.logger.info(f"{self._get_search_id_prefix()} Search name: {search_name}")
            elif search_id:
                self.logger.info(f"{self._get_search_id_prefix()} Search ID: {search_id}")
            self.logger.info(f"{self._get_search_id_prefix()} Number of results: {len(results)}")
            self.logger.info(f"{self._get_search_id_prefix()} Configuration validation passed for both parameters")
            
            success = self.processor.process_multiple_workergroups(
                workergroups=cribl_worker_group,
                lookup_name=lookup_file_name,
                records=results,
                lookup_mode="auto",
                commit_message=commit_message,
                search_name=search_name,
                search_id=search_id
            )
            
            if success:
                self.logger.info(f"{self._get_search_id_prefix()} Alert action completed successfully.")
            else:
                # Log the failure; the base class will handle the exit code
                self.log_error(f"{self._get_search_id_prefix()} One or more worker group updates failed.")
            
        except Exception as e:
            self.log_error(f"{self._get_search_id_prefix()} Error during alert action execution: {str(e)}")
            self.log_error(f"{self._get_search_id_prefix()} Traceback: {traceback.format_exc()}")
            raise


if __name__ == "__main__":
    try:
        alert_name = os.path.basename(__file__).rstrip(".py")
        exitcode = UpdateCriblLookupAlert("UpdateCriblLookup", alert_name).run(sys.argv)
        sys.exit(exitcode)
        
    except Exception as e:
        # A final catch-all for any exceptions during initialization
        import logging
        logger = logging.getLogger('updatecribllookup')
        logger.error(f"FATAL: Unhandled exception in modular alert: {e}\n{traceback.format_exc()}")
        sys.exit(1)


