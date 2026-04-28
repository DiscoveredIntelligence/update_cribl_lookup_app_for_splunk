#!/usr/bin/env python3

import logging
import splunk.admin as admin


# Configure logging to only show ERROR and above to avoid PersistentScript ERROR messages
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('updatecribllookup')

class UpdateCriblLookupLeadersList(admin.MConfigHandler):
    
    def setup(self):
        pass

    def handleList(self, confInfo):
        """Handles listing of leaders."""
        
        try:
            confDict = self.readConf("updatecribllookup_leaders")
            
            if confDict is not None:
                for stanza, settings in confDict.items():
                    confInfo[stanza].append("value", stanza)
                    confInfo[stanza].append("label", stanza)
            
        except Exception as e:
            logger.error(f"leaders_list: Exception in handleList: {str(e)}")
            raise


admin.init(UpdateCriblLookupLeadersList, admin.CONTEXT_APP_AND_USER)


