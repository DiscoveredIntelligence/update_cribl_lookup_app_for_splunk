#!/usr/bin/env python3

import logging
import splunk.admin as admin


# Configure logging to only show ERROR and above to avoid PersistentScript ERROR messages
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger('updatecribllookup')

class UpdateCriblLookupWorkergroupsList(admin.MConfigHandler):

    def setup(self):
        pass

    def handleList(self, confInfo):
        """Handles listing of worker groups from local configuration."""
        
        try:
            confDict = self.readConf("updatecribllookup_workergroups")
            
            if confDict is not None:
                for stanza, settings in confDict.items():
                    if settings.get('enabled', '1') == '1':
                        confInfo[stanza].append("value", stanza)
                        confInfo[stanza].append("label", stanza)
            
        except Exception as e:
            logger.error(f"workergroups_list: Exception in handleList: {str(e)}")
            raise


admin.init(UpdateCriblLookupWorkergroupsList, admin.CONTEXT_APP_AND_USER)

