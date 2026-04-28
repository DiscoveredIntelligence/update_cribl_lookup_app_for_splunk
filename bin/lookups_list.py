#!/usr/bin/env python3

import logging
import splunk.admin as admin


# Configure logging to only show ERROR and above to avoid PersistentScript ERROR messages
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('updatecribllookup')

class UpdateCriblLookupLookupsList(admin.MConfigHandler):

    def setup(self):
        pass

    def handleList(self, confInfo):
        """Handles listing of lookups from local configuration."""
        
        try:
            confDict = self.readConfCtx("updatecribllookup_lookups")
            
            if confDict is not None:
                for stanza, settings in confDict.items():
                    if settings.get('enabled', '1') == '1':
                        confInfo[stanza].append("value", stanza)
                        confInfo[stanza].append("label", stanza)
            
        except Exception as e:
            logger.error(f"lookups_list: Exception in handleList: {str(e)}")
            raise


admin.init(UpdateCriblLookupLookupsList, admin.CONTEXT_APP_AND_USER)


