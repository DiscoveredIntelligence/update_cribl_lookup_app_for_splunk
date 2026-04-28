# UpdateCriblLookup

A Splunk app that provides a custom streaming command and modular alert action to sync Splunk search results to Cribl 
lookups.

## Overview

The `updatecribllookup` streaming command and "Update Cribl Lookup" alert action allow you to take the results from 
any Splunk search and sync them directly to a Cribl Cloud lookup table. This enables real-time updates to Cribl lookups 
based on dynamic Splunk data, making it ideal for maintaining up-to-date reference data, security lists, or 
configuration tables.

## Features

  - **Streaming Command**: Processes search results in real-time as they flow through the search pipeline
  - **Modular Alert Action**: Automatically updates Cribl lookups when alerts are triggered with dynamic UI dropdowns
  - **Chunked Processing**: Handles large datasets efficiently by accumulating records in memory across multiple chunks
  - **Lookup Mode Support**: Supports both memory and disk-based lookups in Cribl with automatic mode detection
  - **Robust Error Handling**: Validation of workergroup and lookup configurations
  - **Configurable Logging**: Adjustable log levels for debugging and monitoring
  - **Dynamic UI**: Alert action form with dynamically populated dropdowns for worker groups and lookups defined in 
      configuration
  - **Shared Processing**: Common processing logic shared between streaming command and alert action
  - **REST Endpoints**: Custom REST endpoints for populating UI dropdowns

## Installation

  1. Deploy the app to your Splunk environment
  2. Configure Cribl leaders and workergroups through the app's configuration interface
  3. Define lookup configurations for your target Cribl lookups

## Configuration

### Cribl Leaders
  Configure your Cribl Cloud leaders with:
  - **Name**: Unique identifier for the leader
  - **Cribl Cloud URL**: Your Cribl Cloud instance URL (e.g., `https://your-org.cribl.cloud`) 
  - **Client ID**: OAuth client ID for authentication
  - **Client Secret**: OAuth client secret for authentication
  - **Enabled**: Toggle to enable/disable the leader

### Workergroups
  Configure workergroups with:
  - **Name**: Workergroup name (use `cribl_default` for the default workergroup)
  - **Associated Leader**: Reference to the configured leader
  - **Enabled**: Toggle to enable/disable the workergroup

### Lookups
  Configure lookup definitions with:
  - **Name**: Lookup filename (e.g., `security_list.csv`)
  - **Enabled**: Toggle to enable/disable the lookup

## Usage

### Streaming Command

#### Basic Syntax
```spl
| search_command | updatecribllookup workergroup=<workergroup_name> lookup=<lookup_name>
```

#### Parameters

##### workergroup (required)
The name of the Cribl workergroup configuration to use. Use `cribl_default` for the default workergroup.

**Example**: `workergroup=cribl_default`

##### lookup (required)
The name of the lookup file to update in Cribl.

**Example**: `lookup=security_threats.csv`

##### lookup_mode (optional)
Specifies the lookup mode in Cribl:
- `auto` (default): Preserve existing lookup mode
- `memory`: Store lookup in memory for fast access
- `disk`: Store lookup on disk for persistence

**Example**: `lookup_mode=memory`

#### Examples

##### Update a security threats lookup
```spl
| inputlookup threat_intelligence.csv
| where threat_level="high"
| updatecribllookup workergroup=cribl_default lookup=active_threats.csv lookup_mode=memory
```

##### Sync user access data to Cribl
```spl
| search index=access_logs action=login
| stats latest(_time) as last_login by user
| updatecribllookup workergroup=security_team lookup=user_access.csv
```

### Alert Action

The "Update Cribl Lookup" modular alert action automatically updates Cribl lookups when alerts are triggered. 
The alert action features a dynamic UI with dropdowns that are populated from the TA's configuration via custom 
REST endpoints.

#### Configuration

When creating or editing an alert, select "Update Cribl Lookup" as an action and configure:

- **Worker Group**: Select from a dynamically populated dropdown of configured and enabled Cribl worker groups
- **Lookup File**: Select from a dynamically populated dropdown of configured and enabled lookup definitions

The dropdowns are populated by REST endpoints:
- `/servicesNS/nobody/UpdateCriblLookup/workergroups_list` - Returns enabled worker groups
- `/servicesNS/nobody/UpdateCriblLookup/lookups_list` - Returns enabled lookup definitions

#### Technical Implementation

The alert action:
  1. Receives search results from the triggering alert as JSON
  2. Extracts the configured worker group and lookup file parameters
  3. Uses the shared `CriblLookupProcessor` class for consistent processing
  4. Validates parameters against the App configuration
  5. Processes the search results and updates the specified Cribl lookup
  6. Updates the single selected worker group
  7. Logs all operations for debugging and monitoring

#### Use Cases

##### Security Incident Response
```spl
| search index=security_alerts severity=high
| stats latest(_time) as last_seen by threat_indicator, threat_type
| where last_seen > relative_time(now(), "-1h")
```
Configure alert to trigger when new high-severity threats are detected, automatically updating the `active_threats.csv` lookup in the security worker group.

##### User Access Monitoring
```spl
| search index=access_logs action=failed_login
| stats count by user, src_ip
| where count > 5
```
Set up alert to trigger on multiple failed login attempts, updating the `suspicious_users.csv` lookup for immediate action.

#### Technical Notes

- The alert action uses the same underlying `CriblLookupProcessor` class as the streaming command
- All search results from the triggering search are passed to the lookup update
- Only one worker group can be selected and updated per alert action
- Lookup mode is determined by the lookup configuration (defaults to "auto")
- The alert action runs with python3 and uses JSON payload format
- Error handling and logging are consistent with the streaming command

## Architecture

### Shared Processing Class

Both the streaming command and alert action use the `CriblLookupProcessor` class (`cribl_lookup_processor.py`) which provides:

- **Unified Validation**: Consistent parameter validation for both command and alert action
- **Configuration Management**: Centralized access to App configuration settings
- **Error Handling**: Common exception handling and logging
- **Cribl API Integration**: Shared authentication and API interaction logic
- **Record Processing**: Common CSV formatting and data transformation
- **Workergroup Support**: Ability to update worker groups via API integration

### REST Endpoints

The App exposes custom REST endpoints for dynamic UI population:

  #### `/servicesNS/nobody/UpdateCriblLookup/leaders_list`
  Returns enabled Cribl leaders with their configuration details.

  #### `/servicesNS/nobody/UpdateCriblLookup/workergroups_list`
  Returns enabled worker groups with their associated leaders.

  #### `/servicesNS/nobody/UpdateCriblLookup/lookups_list`
  Returns enabled lookup definitions for dropdown population.

  These endpoints are implemented as custom REST handlers and are secured through Splunk's authentication system.

#### Using REST Endpoints in Search

  You can query these endpoints directly in Splunk searches to list configured items:

##### List Leaders
  ```spl
  | rest /servicesNS/nobody/UpdateCriblLookup/leaders_list
  ```
  Returns enabled Cribl leaders with fields: `name`, `cribl_cloud_url`, `enabled`

##### List Worker Groups
  ```spl
  | rest /servicesNS/nobody/UpdateCriblLookup/workergroups_list
  ```
  Returns enabled worker groups with fields: `name`, `associated_leader_name`, `enabled`

##### List Lookups
  ```spl
  | rest /servicesNS/nobody/UpdateCriblLookup/lookups_list
  ```
  Returns enabled lookup definitions with fields: `name`, `enabled`

##### Example: View All Configuration
  ```spl
  | rest /servicesNS/nobody/UpdateCriblLookup/leaders_list
  | eval config_type="leaders"
  | append [| rest /servicesNS/nobody/UpdateCriblLookup/workergroups_list | eval config_type="workergroups"]
  | append [| rest /servicesNS/nobody/UpdateCriblLookup/lookups_list | eval config_type="lookups"]
  | table config_type, name, *
  ```

## How It Works

### Streaming Command Flow
  1. **Initialization**: Command initializes with session key and creates `CriblLookupProcessor` instance
  2. **Parameter Validation**: Validates workergroup and lookup parameters against app configuration
  3. **Chunked Processing**: Accumulates records from multiple chunks from search ID 
  4. **Final Processing**: When final chunk is detected, processes all accumulated records
  5. **Data Transformation**: Converts search results to CSV format for Cribl
  6. **Sync Operation**: Uploads data to specified Cribl lookup via Cloud API
  7. **Passthrough**: Original search results are passed through unchanged

### Alert Action Flow
  1. **Input Processing**: Reads JSON configuration and search results 
  2. **Parameter Extraction**: Extracts worker group and lookup file parameters
  3. **Processing**: Processes all results at once 
  4. **Worker Group Update**: Updates the selected worker group
  5. **Result Logging**: Logs success/failure for monitoring

## Technical Details

### Streaming vs Batch Processing
The streaming command uses Splunk's streaming architecture:
  - Processes data as it flows through the pipeline
  - Handles large datasets by processing in manageable chunks

### Workergroup Name Mapping
Since "default" is a reserved word in Splunk, the command automatically maps:
- `cribl_default` (Splunk configuration) → `default` (Cribl API)
- Other workergroup names are passed through unchanged

### Error Handling
Comprehensive error handling includes:
  - **Parameter Validation**: Missing or invalid parameters
  - **Configuration Validation**: Non-existent workergroup or lookup configurations
  - **Authentication Errors**: Cribl API authentication failures
  - **Network Issues**: API connectivity problems
  - **Data Processing Errors**: CSV formatting or data transformation issues

### Logging
  The App uses Python's logging module with configurable levels:
  - **Logger Name**: `updatecribllookup` 
  - **Log Levels**: INFO, DEBUG, ERROR (configurable via app settings)
  - **Log Location**: `$SPLUNK_HOME/var/log/splunk/updatecribllookup.log`
                      `index=_internal sourcetype=updatecribllookup`
  - **Log Content**: Operation status, record counts, API calls, error details

## Troubleshooting

### Common Issues

#### "WorkergroupNotFoundException"
  - Verify the workergroup exists in the app configuration
  - Check that the workergroup is enabled in the configuration
  - Ensure the workergroup name is spelled correctly
  - Verify the associated leader is properly configured and enabled

#### "LookupNotFoundException"
  - Confirm the lookup is defined in the app configuration
  - Check that the lookup is enabled in the configuration
  - Verify the lookup filename includes the .csv extension
  - Ensure the lookup exists in the target Cribl workergroup

#### "Authentication failed"
  - Verify the Cribl leader credentials (client ID and secret)
  - Check that the Cribl Cloud URL is correct and accessible
  - Ensure the OAuth credentials have appropriate permissions
  - Verify the leader configuration is enabled

#### Alert Action Dropdowns Not Populated
  - Check Splunk logs for REST endpoint errors

#### "No session key available"
  - This typically occurs in alert actions when session context is lost
  - Check that the alert action is properly configured 

### Performance Considerations
  - **Chunked Processing**: Automatically handles large result sets efficiently
  - **API Rate Limits**: Respects Cribl Cloud API rate limits
  - **Single Worker Group**: Alert action updates one worker group per execution

### Debug Mode

To enable debug logging:
1. Access the app settings tab
2. Set log level to DEBUG
4. Check logs at `$SPLUNK_HOME/var/log/splunk/updatecribllookup.log` 
  or via search: `index=_internal sourcetype=updatecribllookup`

## File Structure

```
UpdateCriblLookup/
├── bin/
│   ├── updatecribllookup.py                       # Streaming command
│   ├── update_cribl_lookup.py                     # Alert action
│   ├── cribl_lookup_processor.py                  # Shared processing class
│   ├── leaders_list.py                            # REST handler for leaders
│   ├── workergroups_list.py                       # REST handler for workergroups
│   ├── lookups_list.py                            # REST handler for lookups
│   ├── cribl_sync_engine.py                       # Cribl API integration
│   ├── ucl_config.py                              # Configuration management
│   ├── updatecribllookup_exceptions.py            # Custom exceptions
│   ├── import_declare_test.py                     # Import declarations
│   ├── updatecribllookup_rh_leaders.py            # UCC REST handler for leaders
│   ├── updatecribllookup_rh_lookups.py            # UCC REST handler for lookups
│   ├── updatecribllookup_rh_settings.py           # UCC REST handler for settings
│   └── updatecribllookup_rh_workergroups.py       # UCC REST handler for workergroups
├── default/
│   ├── alert_actions.conf                        # Alert action configuration
│   ├── app.conf                                  # App configuration
│   ├── commands.conf                             # Command configuration
│   ├── restmap.conf                              # REST endpoint mappings
│   ├── server.conf                               # Server configuration
│   ├── updatecribllookup_settings.conf           # App settings
│   ├── web.conf                                  # Web configuration
│   └── data/
│       └── ui/
│           ├── alerts/
│           │   └── update_cribl_lookup.xml       # Alert action UI form
├── README/
│   ├── alert_actions.conf.spec                   # Alert action spec
│   ├── updatecribllookup_account.conf.spec       # Account configuration spec
│   ├── updatecribllookup_leaders.conf.spec       # Leaders configuration spec
│   ├── updatecribllookup_lookups.spec            # Lookups configuration spec
│   ├── updatecribllookup_settings.conf.spec      # Settings configuration spec
│   └── updatecribllookup_workergroups.conf.spec  # Workergroups configuration spec
├── appserver/
│   ├── static/                                   # Static web assets
│   └── templates/                                # Web templates
├── lib/                                          # Third-party libraries
│   ├── certifi/                                  # SSL certificates
│   ├── requests/                                 # HTTP library
│   ├── splunklib/                                # Splunk SDK
│   ├── splunktaucclib/                           # UCC framework
│   ├── solnlib/                                  # Splunk solution library
│   └── requirements.txt                          # Python dependencies
├── metadata/
│   └── default.meta                              # Metadata configuration
├── static/
│   ├── appIcon.png                               # App icon
│   ├── appIcon_2x.png                            # App icon (2x)
│   ├── appIconAlt.png                            # App icon (alt)
│   └── appIconAlt_2x.png                         # App icon (alt 2x)
├── app.manifest                                  # App manifest
├── VERSION                                       # Version file
└── README.md                                     # App README file
```

## API Integration

The App integrates with the Cribl Cloud API through:

### Authentication
- **OAuth 2.0**: Uses client credentials flow
- **Token Management**: Automatic token refresh and caching
- **Secure Storage**: Credentials stored in Splunk's credential store

### API Endpoints
- **Authentication**: `/api/v1/auth/login`
- **Lookup Upload**: `/api/v1/system/lookups/{lookup_name}`
- **Workergroup Management**: Integration with Cribl's workergroup APIs

### Error Handling
- **Rate Limiting**: Automatic retry with exponential backoff
- **Network Errors**: Graceful handling of connectivity issues
- **API Errors**: Detailed error logging and user feedback

## Security

- **OAuth Credentials**: Stored securely in Splunk's credential store
- **HTTPS Communication**: All API communications use HTTPS
- **No Sensitive Logging**: Credentials and sensitive data are not logged
- **Input Validation**: All user inputs are validated before processing
- **Permission Checks**: REST endpoints respect Splunk's role-based access control

## Development

### Prerequisites
- Python 3.7+
- Splunk SDK for Python
- splunktaucclib (for UCC framework)
- requests library

### Testing
- Unit tests for individual components
- Integration tests with Cribl Cloud API
- UI testing for alert action forms
- Performance testing with large datasets

### Contributing
1. Follow existing code style and patterns
2. Add appropriate logging for new features
3. Update documentation for any changes
4. Test thoroughly before submitting changes

## Support

For issues and questions:
1. Check the App logs for detailed error information
2. Verify configuration settings in the App interface
3. Test connectivity to Cribl Cloud using the streaming command
4. Review Cribl workergroup and lookup configurations
5. Check Splunk's internal logs for REST endpoint issues

Contact: support@discoveredintelligence.ca


