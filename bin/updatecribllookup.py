#!/usr/bin/env python3

import import_declare_test 
import sys
import time

try:
    from splunklib.searchcommands import dispatch, EventingCommand, Configuration, Option, validators
    from UpdateCriblLookup.cribl_lookup_processor import CriblLookupProcessor
    
except ImportError as e:
    print(f"Error importing required modules: {e}", file=sys.stderr)
    sys.exit(1)


@Configuration()
class UpdateCriblLookupCommand(EventingCommand):
    """
    An eventing command that updates Cribl lookups with complete dataset processing.
    Uses the transform() generator method to yield progress records in real-time, 
    keeping the search alive during long sync operations.
    
    Usage:
        | makeresults | updatecribllookup workergroup=cribl_default lookup=test1.csv
        | makeresults | updatecribllookup workergroup=cribl_default lookup=test1.csv lookup_mode=memory
        | makeresults | updatecribllookup workergroup=cribl_default lookup=test1.csv lookup_mode=disk
    """
    

    workergroup = Option(
        doc='''
        **Syntax:** **workergroup=***<workergroup_name>*
        **Description:** Name of the Cribl worker group configuration to use''',
        require=False, validate=validators.Fieldname())
    

    lookup = Option(
        doc='''
        **Syntax:** **lookup=***<lookup_name>*
        **Description:** Name of the lookup file to update''',
        require=False, validate=validators.Fieldname())
    

    lookup_mode = Option(
        doc='''
        **Syntax:** **lookup_mode=***<mode>*
        **Description:** Lookup mode: "auto" (preserve existing), "memory", or "disk". Default is "auto"''',
        require=False, validate=validators.Match("lookup_mode", r"^(auto|memory|disk)$"))
    

    commit_message = Option(
        doc='''
        **Syntax:** **commit_message=***<message>*
        **Description:** Optional custom commit message for the Cribl deployment''',
        require=False)


    def __init__(self):
        super(UpdateCriblLookupCommand, self).__init__()
        self.processor = None
        self.search_id = None
        self.owner = None
        self.search_name = None
    

    def _get_search_id_prefix(self):
        """
        Get search ID prefix for logging consistency.
        Returns search ID prefix if available, empty string otherwise.
        """
        if self.search_id:
            return f"({self.search_id}) "
        return ""
    

    def transform(self, records):
        """
        Process the complete dataset while yielding progress records in real-time.
        The transform() method is a generator, allowing us to yield records during 
        long operations to keep the search alive.
        """
        try:
            # FIRST: Get search ID and search name for logging - do this before ANY other actions
            if hasattr(self, '_metadata') and hasattr(self._metadata, 'searchinfo'):
                self.search_id = getattr(self._metadata.searchinfo, 'sid', None)
                self.owner = getattr(self._metadata.searchinfo, 'username', None)
            
            # Extract search name from search_results_info if available
            if hasattr(self, 'search_results_info') and self.search_results_info:
                # Check for saved search name (common field names)
                search_info = self.search_results_info
                self.search_name = (getattr(search_info, 'label', None) or 
                                    getattr(search_info, 'savedsearch_name', None) or 
                                    getattr(search_info, 'saved_search', None))
            
            # Collect all records (for complete dataset processing)
            record_list = list(records)
            record_count = len(record_list)
            
            # Initialize processor NOW with search ID for proper logging context from the start
            session_key = self.service.token
            self.processor = CriblLookupProcessor(session_key, 'updatecribllookup_command', self.search_id)
            
            # Validate parameters
            self.processor.validate_parameters(self.workergroup, self.lookup)
            
            # Log startup info
            msg = f"Starting updatecribllookup command (workergroup: {self.workergroup}, lookup: {self.lookup})"
            self.processor.logger.info(f"{self._get_search_id_prefix()} {msg}")
            self.processor.logger.info(f"{self._get_search_id_prefix()} Configuration validation passed for all " \
                                       f"parameters")
            self.processor.logger.info(f"{self._get_search_id_prefix()} Processing complete dataset with " \
                                       f"{record_count} records")
            self.processor.logger.debug(f"{self._get_search_id_prefix()} SID: {self.search_id} Owner: {self.owner}")

            # Yield a progress record to show the command is starting
            yield {
                '_time': int(time.time()),
                'cribl_sync_status': 'starting',
                'cribl_sync_message': f'Starting Cribl sync for {record_count} records',
                'cribl_sync_lookup': self.lookup,
                'cribl_sync_workergroup': self.workergroup
            }
            
            # Process the lookup update with the complete dataset
            lookup_mode = self.lookup_mode or "auto"
            
            # Process the sync operation as a generator, yielding progress records
            sync_generator = self._process_sync_with_progress(
                record_list, lookup_mode
            )
            
            # Yield all progress records and results from the sync
            for record in sync_generator:
                yield record
                
        except Exception as e:
            if self.processor:
                self.processor.logger.error(f"{self._get_search_id_prefix()} Error in transform method: {str(e)}")
            raise
    

    def _process_sync_with_progress(self, record_list, lookup_mode):
        """
        Generator method that performs the sync operation while yielding progress records.
        Uses a non-blocking approach to prevent search auto-cancellation.
        """
        record_count = len(record_list)
        
        try:
            # Start progress
            yield {
                '_time': time.time(),
                'cribl_sync_status': 'initializing',
                'cribl_sync_message': 'Starting Cribl sync operation',
                'cribl_sync_lookup': self.lookup,
                'cribl_sync_workergroup': self.workergroup
            }
            
            # Use generator-based sync to yield control frequently
            sync_generator = self.processor.process_lookup_update_with_progress(
                workergroup=self.workergroup,
                lookup_name=self.lookup,
                records=record_list,
                lookup_mode=lookup_mode,
                commit_message=getattr(self, 'commit_message', None),
                search_name=self.search_name,
                search_id=self.search_id
            )
            
            # Process sync operations one step at a time, yielding progress
            sync_success = False
            final_result = None
            
            for progress_info in sync_generator:
                progress_type = progress_info.get('type', 'unknown')
                
                if progress_type == 'success':
                    sync_success = True
                    final_result = progress_info
                    # Don't yield this final success message yet, we'll create our own
                elif progress_type == 'error':
                    sync_success = False
                    final_result = progress_info
                    # Don't yield this final error message yet, we'll create our own
                else:
                    # Yield the progress info from sync engine with consistent cribl_sync_* fields
                    yield {
                        '_time': time.time(),
                        'cribl_sync_status': 'processing',
                        'cribl_sync_message': progress_info.get('message', 'Processing...'),
                        'cribl_sync_lookup': self.lookup,
                        'cribl_sync_workergroup': self.workergroup,
                        'progress_type': progress_type
                    }
            
            # Yield final result based on actual success/failure
            if sync_success:
                yield {
                    '_time': int(time.time()),
                    'cribl_sync_status': 'completed',
                    'cribl_sync_message': f'Completed Cribl sync for {record_count} records',
                    'cribl_sync_lookup': self.lookup,
                    'cribl_sync_workergroup': self.workergroup
                }
            else:
                error_msg = final_result.get('message', 'Unknown error') if final_result else 'Sync operation failed'
                yield {
                    '_time': int(time.time()),
                    'cribl_sync_status': 'error',
                    'cribl_sync_message': f'Failed to sync {record_count} records: {error_msg}',
                    'cribl_sync_lookup': self.lookup,
                    'cribl_sync_workergroup': self.workergroup
                }
                
        except Exception as e:
            yield {
                '_time': time.time(),
                'cribl_sync_status': 'error',
                'cribl_sync_message': f'Sync failed: {str(e)}',
                'cribl_sync_lookup': self.lookup,
                'cribl_sync_workergroup': self.workergroup
            }
            raise


dispatch(UpdateCriblLookupCommand, sys.argv, sys.stdin, sys.stdout, __name__)

